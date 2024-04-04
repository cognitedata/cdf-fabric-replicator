import json
import logging
import time

from typing import Any, Dict, List
from cognite.extractorutils.base import CancellationToken

from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.query import SourceSelector
from cognite.client.data_classes.data_modeling.query import Query, Select, NodeResultSetExpression, QueryResult, EdgeResultSetExpression
from cognite.client.data_classes.filters import HasData, Equals
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.base import Extractor

from azure.identity import DefaultAzureCredential
from deltalake import write_deltalake
import pyarrow as pa

from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config, DataModelingConfig
from cdf_fabric_replicator.metrics import Metrics


class DataModelingReplicator(Extractor):
    def __init__(self, metrics: Metrics, stop_event: CancellationToken) -> None:
        super().__init__(
            name="cdf_fabric_replicator_data_modeling",
            description="CDF Fabric Replicator",
            config_class=Config,
            metrics=metrics,
            use_default_state_store=False,
            version=__version__,
            cancellation_token=stop_event,
        )
        self.metrics: Metrics
        #self.stop_event = stop_event
        self.endpoint_source_map: Dict[str, Any] = {}
        self.errors: List[str] = []
        self.azure_credential = DefaultAzureCredential()

    def run(self) -> None:
        # init/connect to destination
        self.state_store.initialize()
        
        if self.config.data_modeling is None:         
            logging.info("No data modeling spaces found in config")
            return
        
        while True: #not self.stop_event.is_set():
            start_time = time.time()  # Get the current time in seconds

            self.process_spaces()

            end_time = time.time()  # Get the time after function execution
            elapsed_time = end_time - start_time
            sleep_time = max(self.config.extractor.poll_time - elapsed_time, 0)  # 900s = 15min
            if sleep_time > 0:
                logging.debug(f"Sleep for {sleep_time} seconds")
                time.sleep(sleep_time)

    def process_spaces(self) -> None:
        for data_model_config in self.config.data_modeling:
            all_views = self.cognite_client.data_modeling.views.list(space=data_model_config.space, limit=-1)
            views_dict = all_views.dump()

            for view in views_dict:
                state_id = f"state_{data_model_config.space}_{view['externalId']}_{view['version']}"
                self.process_instances(data_model_config, state_id, view)
            
            # edge with no view 
            state_id = f"state_{data_model_config.space}_edges"
            self.process_instances(data_model_config, state_id)


    def process_instances(self, data_model_config: DataModelingConfig, state_id: str, view: Dict[str, Any]=None) -> None:
        if view:
            query = self.generate_query_based_on_view(view)
        else:
            query = self.generate_query_based_on_edge()
        self.write_instance_to_lakehouse(data_model_config, state_id, query)


    def generate_query_based_on_view(self, view: Dict[str, Any]) -> Query:
        view_properties = list(view["properties"].keys())
        view_id = ViewId(space=view["space"], external_id=view["externalId"], version=view["version"])

        if view["usedFor"] != "edge":
            query = Query(
                with_ = {
                    "nodes": NodeResultSetExpression(filter=HasData(views=[view_id])),
                },
                select = {
                    "nodes": Select([SourceSelector(source=view_id, properties=view_properties)]),
                }
            )
        else:
            query = Query(
                with_ = {
                    "edges": EdgeResultSetExpression(filter=Equals(["edge", "type"], {"space": view_id.space, "externalId": view_id.external_id})),
                },
                select = {
                    "edges": Select([SourceSelector(source=view_id, properties=view_properties)]),
                }
            )
        return query
    

    def generate_query_based_on_edge(self) -> Query:
        query = Query(
                with_ = {
                    "edges": EdgeResultSetExpression(),
                },
                select = {
                    "edges": Select(),
                }
            )
        return query


    def write_instance_to_lakehouse(self, data_model_config: DataModelingConfig, state_id: str, query: Query) -> None:
        cursors = self.state_store.get_state(external_id=state_id)[1]

        if cursors:
            query.cursors = json.loads(cursors)

        try:
            res = self.cognite_client.data_modeling.instances.sync(query=query)
        except CogniteAPIError:
            query.cursors = None
            res = self.cognite_client.data_modeling.instances.sync(query=query)
            
        self.send_to_lakehouse(data_model_config=data_model_config, state_id=state_id, result=res)
        
        while ("nodes" in res.data and len(res.data["nodes"]) > 0) or ("edges" in res.data and len(res.data["edges"])) > 0:
            query.cursors = res.cursors
            
            try:
                res = self.cognite_client.data_modeling.instances.sync(query=query)
            except CogniteAPIError:
                query.cursors = None
                res = self.cognite_client.data_modeling.instances.sync(query=query)
                
            self.send_to_lakehouse(data_model_config=data_model_config, state_id=state_id, result=res)

        self.state_store.set_state(external_id=state_id, high=json.dumps(res.cursors))
        self.state_store.synchronize()

    def send_to_lakehouse(
        self,
        data_model_config: DataModelingConfig,
        state_id: str,
        result: QueryResult,
    ) -> None:
        logging.debug(f"Ingest to lakehouse {state_id}")

        nodes = self.get_instances(result, is_edge=False)
        edges = self.get_instances(result, is_edge=True)
        
        abfss_prefix = data_model_config.lakehouse_abfss_prefix
        if (len(nodes) > 0):
            self.write_instances_to_lakehouse_tables(nodes, abfss_prefix)

        if (len(edges) > 0):
            self.write_instances_to_lakehouse_tables(edges, abfss_prefix)


    def get_instances(self, result: QueryResult, is_edge=False) -> Dict[str, Any]:
        instances = {}
        if (is_edge and "edges" not in result) or (not is_edge and "nodes" not in result):
            return instances
        
        instance_type = "edge" if is_edge else "node"

        if is_edge:
            instances_from_result = result.get_edges("edges")
        else:
            instances_from_result = result.get_nodes("nodes")
        for instance in instances_from_result:
                item = {
                    "space": instance.space,
                    "instanceType": instance_type,
                    "externalId": instance.external_id,
                    "version": instance.version,
                    "lastUpdatedTime": instance.last_updated_time,
                    "createdTime": instance.created_time,
                }

                if is_edge:
                    item["startNode"] = {"space": instance.start_node.space, "externalId": instance.start_node.external_id }
                    item["endNode"] = {"space": instance.end_node.space, "externalId": instance.end_node.external_id }

                for view in instance.properties.data:
                    propDict = instance.properties.data[view]
                    item.update(propDict)

                table_name = f"{instance.space}_edges" if is_edge else f"{view.space}_{view.external_id}"
                if table_name not in instances:
                    instances[table_name] = [item]
                else:
                    instances[table_name].append(item)
        return instances
    

    def write_instances_to_lakehouse_tables(self, instances: Dict[str, Any], abfss_prefix: str) -> None:
        token = self.azure_credential.get_token("https://storage.azure.com/.default")
        for table in instances:
            abfss_path = f"{abfss_prefix}/Tables/{table}"
            logging.info(f"Writing {len(instances[table])} to '{abfss_path}' table...")
            data = pa.Table.from_pylist(instances[table])
            write_deltalake(abfss_path, data, engine="rust", mode="append", schema_mode="merge", storage_options={"bearer_token": token.token, "use_fabric_endpoint": "true"})
            logging.info("done.")