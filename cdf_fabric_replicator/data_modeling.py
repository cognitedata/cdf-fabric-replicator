# mypy: ignore-errors
import json
import logging
import time

from typing import Any, Dict, List, Optional
from cognite.extractorutils.base import CancellationToken

from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import (
    NodeList,
    EdgeList,
)
from cognite.client.data_classes.data_modeling.query import (
    Query,
    Select,
    SourceSelector,
    NodeResultSetExpression,
    QueryResult,
    EdgeResultSetExpression,
)
from cognite.client.data_classes.filters import HasData, Equals, Not, MatchAll
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.base import Extractor

from azure.identity import DefaultAzureCredential
from deltalake.exceptions import DeltaError
from deltalake import write_deltalake
import pyarrow as pa

from cdf_fabric_replicator import __version__ as fabric_replicator_version
from cdf_fabric_replicator.config import Config, DataModelingConfig
from cdf_fabric_replicator.metrics import Metrics


class DataModelingReplicator(Extractor):
    def __init__(
        self,
        metrics: Metrics,
        stop_event: CancellationToken,
        override_config_path: Optional[str] = None,
    ) -> None:
        super().__init__(
            name="cdf_fabric_replicator_data_modeling",
            description="CDF Fabric Replicator",
            config_class=Config,
            metrics=metrics,
            use_default_state_store=False,
            version=fabric_replicator_version,
            cancellation_token=stop_event,
            config_file_path=override_config_path,
        )
        self.metrics: Metrics
        self.stop_event = stop_event
        self.endpoint_source_map: Dict[str, Any] = {}
        self.errors: List[str] = []
        self.azure_credential = DefaultAzureCredential()
        self.logger = logging.getLogger(self.name)

    def run(self) -> None:
        # init/connect to destination
        self.state_store.initialize()

        self.logger.debug(f"Current Data Modeling Config: {self.config.data_modeling}")

        if self.config.data_modeling is None:
            self.logger.info("No data modeling spaces found in config")
            return

        while not self.stop_event.is_set():
            start_time = time.time()  # Get the current time in seconds

            self.process_spaces()

            end_time = time.time()  # Get the time after function execution
            elapsed_time = end_time - start_time
            sleep_time = max(
                self.config.extractor.poll_time - elapsed_time, 0
            )  # 900s = 15min
            if sleep_time > 0:
                self.logger.debug(f"Sleep for {sleep_time} seconds")
                self.stop_event.wait(sleep_time)

        self.logger.info("Stop event set. Exiting...")

    def process_spaces(self) -> None:
        for data_model_config in self.config.data_modeling:
            try:
                all_views = self.cognite_client.data_modeling.views.list(
                    space=data_model_config.space, limit=-1
                )
            except CogniteAPIError as e:
                self.logger.error(
                    f"Failed to list views for space {data_model_config.space}. Error: {e}"
                )
                raise e

            views_dict = all_views.dump()

            for view in views_dict:
                state_id = f"state_{data_model_config.space}_{view['externalId']}_{view['version']}"
                self.process_instances(data_model_config, state_id, view)

            # edge with no view
            state_id = f"state_{data_model_config.space}_edges"
            self.process_instances(data_model_config, state_id)

    def process_instances(
        self,
        data_model_config: DataModelingConfig,
        state_id: str,
        view: Dict[str, Any] = {},
    ) -> None:
        if view:
            query = self.generate_query_based_on_view(view)
        else:
            query = self.generate_query_based_on_edge()
        self.write_instance_to_lakehouse(data_model_config, state_id, query)

    def generate_query_based_on_view(self, view: Dict[str, Any]) -> Query:
        view_properties = list(view["properties"].keys())
        view_id = ViewId(
            space=view["space"], external_id=view["externalId"], version=view["version"]
        )

        if view["usedFor"] != "edge":
            query = Query(
                with_={
                    "nodes": NodeResultSetExpression(filter=HasData(views=[view_id])),
                },
                select={
                    "nodes": Select(
                        [SourceSelector(source=view_id, properties=view_properties)]
                    ),
                },
            )
        else:
            query = Query(
                with_={
                    "edges": EdgeResultSetExpression(
                        filter=Equals(
                            ["edge", "type"],
                            {"space": view_id.space, "externalId": view_id.external_id},
                        )
                    ),
                },
                select={
                    "edges": Select(
                        [SourceSelector(source=view_id, properties=view_properties)]
                    ),
                },
            )
        return query

    def generate_query_based_on_edge(self) -> Query:
        query = Query(
            with_={
                "edges": EdgeResultSetExpression(),
            },
            select={
                "edges": Select(),
            },
        )
        return query

    def write_instance_to_lakehouse(
        self, data_model_config: DataModelingConfig, state_id: str, query: Query
    ) -> None:
        cursors = self.state_store.get_state(external_id=state_id)[1]
        if cursors:
            query.cursors = json.loads(str(cursors))
        elif state_id.endswith("_edges"):
            query.with_ = {"edges": EdgeResultSetExpression(filter=Not(MatchAll()))}
        else:
            query.with_ = {"nodes": NodeResultSetExpression(filter=Not(MatchAll()))}

        try:
            res = self.cognite_client.data_modeling.instances.sync(query=query)
        except CogniteAPIError:
            query.cursors = None  # type: ignore
            try:
                res = self.cognite_client.data_modeling.instances.sync(query=query)
            except CogniteAPIError as e:
                self.logger.error(f"Failed to sync instances. Error: {e}")
                raise e

        self.send_to_lakehouse(data_model_config=data_model_config, result=res)

        while ("nodes" in res.data and len(res.data["nodes"]) > 0) or (
            "edges" in res.data and len(res.data["edges"])
        ) > 0:
            query.cursors = res.cursors

            try:
                res = self.cognite_client.data_modeling.instances.sync(query=query)
            except CogniteAPIError:
                query.cursors = None  # type: ignore
                try:
                    res = self.cognite_client.data_modeling.instances.sync(query=query)
                except CogniteAPIError as e:
                    self.logger.error(f"Failed to sync instances. Error: {e}")
                    raise e

            self.send_to_lakehouse(data_model_config=data_model_config, result=res)

        if cursors is None:
            # if no cursors, initial load using instances.list
            # this is needed for the first time when there are no cursors

            if "nodes" in query.select:
                sources = query.select["nodes"].sources
                instance_type = "node"
            elif "edges" in query.select:
                sources = query.select["edges"].sources
                instance_type = "edge"

            for chunk in self.cognite_client.data_modeling.instances(
                sources=sources[0].source if sources else None,
                instance_type=instance_type,
                chunk_size=int(self.config.extractor.fabric_ingest_batch_size),
            ):
                if chunk:
                    self.send_to_lakehouse(
                        data_model_config=data_model_config, result=chunk
                    )

        self.state_store.set_state(external_id=state_id, high=json.dumps(res.cursors))
        self.state_store.synchronize()

    def send_to_lakehouse(
        self,
        data_model_config: DataModelingConfig,
        result: QueryResult | NodeList | EdgeList,
    ) -> None:
        # check type of result
        if isinstance(result, QueryResult):
            nodes = self.get_instances(result, is_edge=False)
            edges = self.get_instances(result, is_edge=True)
        elif isinstance(result, NodeList):
            nodes = self.get_instances_from_result(result)
            edges = {}
        elif isinstance(result, EdgeList):
            nodes = {}
            edges = self.get_instances_from_result(result)

        abfss_prefix = data_model_config.lakehouse_abfss_prefix
        if len(nodes) > 0:
            self.write_instances_to_lakehouse_tables(nodes, abfss_prefix)

        if len(edges) > 0:
            self.write_instances_to_lakehouse_tables(edges, abfss_prefix)

    def get_instances(
        self, result: QueryResult, is_edge: bool = False
    ) -> Dict[str, Any]:
        if (is_edge and "edges" not in result) or (
            not is_edge and "nodes" not in result
        ):
            return {}

        if is_edge:
            instances_from_result = result.get_edges("edges")
        else:
            instances_from_result = result.get_nodes("nodes")  # type: ignore

        return self.get_instances_from_result(
            instances_from_result=instances_from_result
        )

    def get_instances_from_result(
        self, instances_from_result: NodeList | EdgeList
    ) -> Dict[str, Any]:
        instances = {}

        instance_type = (
            "edge" if isinstance(instances_from_result, EdgeList) else "node"
        )

        for instance in instances_from_result:
            item = {
                "space": instance.space,
                "instanceType": instance_type,
                "externalId": instance.external_id,
                "version": instance.version,
                "lastUpdatedTime": instance.last_updated_time,
                "createdTime": instance.created_time,
            }

            if instance_type == "edge":
                item["startNode"] = {
                    "space": instance.start_node.space,
                    "externalId": instance.start_node.external_id,
                }
                item["endNode"] = {
                    "space": instance.end_node.space,
                    "externalId": instance.end_node.external_id,
                }

            for view in instance.properties.data:
                propDict = instance.properties.data[view]
                item.update(propDict)

            table_name = (
                f"{instance.space}_edges"
                if instance_type == "edge"
                else f"{view.space}_{view.external_id}"
            )
            if table_name not in instances:
                instances[table_name] = [item]
            else:
                instances[table_name].append(item)
        return instances

    def write_instances_to_lakehouse_tables(
        self, instances: Dict[str, Any], abfss_prefix: str
    ) -> None:
        token = self.azure_credential.get_token("https://storage.azure.com/.default")
        for table in instances:
            abfss_path = f"{abfss_prefix}/Tables/{table}"
            self.logger.info(
                f"Writing {len(instances[table])} to '{abfss_path}' table..."
            )
            data = pa.Table.from_pylist(instances[table])
            try:
                write_deltalake(
                    abfss_path,
                    data,
                    engine="rust",
                    mode="append",
                    schema_mode="merge",
                    storage_options={
                        "bearer_token": token.token,
                        "use_fabric_endpoint": str(
                            self.config.extractor.use_fabric_endpoint
                        ).lower(),
                    },
                )
            except DeltaError as e:
                self.logger.error(f"Error writing instances to lakehouse tables: {e}")
                raise e
            self.logger.info(
                f"Successfully wrote {len(instances[table])} to '{abfss_path}' table."
            )
