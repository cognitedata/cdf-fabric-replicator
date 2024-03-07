import json
import logging
import threading
import time

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.query import SourceSelector
from cognite.client.data_classes.data_modeling.query import Query, Select, NodeResultSetExpression, QueryResult, EdgeResultSetExpression
from cognite.client.data_classes.filters import HasData, Equals
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.base import Extractor

from azure.identity import DefaultAzureCredential
from deltalake import write_deltalake
import pandas as pd
import numpy as np
import pyarrow as pa
from collections import OrderedDict


from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config, DataModelingConfig
from cdf_fabric_replicator.metrics import Metrics


class DataModelingReplicator(Extractor):
    def __init__(self, metrics: Metrics) -> None: #, stop_event: Event) -> None:
        super().__init__(
            name="cdf_fabric_replicator",
            description="CDF Fabric Replicator",
            config_class=Config,
            metrics=metrics,
            use_default_state_store=False,
            version=__version__,
            #cancellation_token=stop_event,
        )
        self.metrics: Metrics
        #self.stop_event = stop_event
        self.endpoint_source_map: Dict[str, Any] = {}
        self.errors: List[str] = []
        self.azure_credential = DefaultAzureCredential()

        self.schema = pa.schema([
                pa.field('space', pa.string()),
                pa.field('instanceType', pa.string()),
                pa.field('externalId', pa.string()),
                pa.field('version', pa.int32()),
                pa.field('lastUpdatedTime', pa.int64()),
                pa.field('createdTime', pa.int64()),
                pa.field('propertyName', pa.string()),
                pa.field('propertyValue', pa.string()),
                pa.field('type', pa.struct([
                    pa.field('space', pa.string()),
                    pa.field('externalId', pa.string()),
                ]))
                ]
                )


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
                logging.info(f"Sleep for {sleep_time} seconds")
                time.sleep(sleep_time)

    def process_spaces(self) -> None:

        for data_model_config in self.config.data_modeling:
            all_views = self.cognite_client.data_modeling.views.list(space=data_model_config.space, limit=-1)
            views_dict = all_views.dump()

            for item in views_dict:
                view_properties = list(item["properties"].keys())
                state_id = f"state_{data_model_config.space}_{item['external_id']}_{item['version']}"
                cursors = None #self.state_store.get_state(external_id=state_id)[1]
                logging.debug(f"State for {state_id} is {cursors}")

                view_id = ViewId(space=item["space"], external_id=item["external_id"], version=item["version"])

                if item["used_for"] != "edge":
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

                if cursors:
                    query.cursors = json.loads(cursors)
                logging.debug(query.dump())

                try:
                    res = self.cognite_client.data_modeling.instances.sync(query=query)
                except CogniteAPIError as e:
                    query.cursors = None
                    res = self.cognite_client.data_modeling.instances.sync(query=query)
                # send to lakehouse
                self.send_to_lakehouse(data_model_config=data_model_config, state_id=state_id, result=res)

                while ("nodes" in res.data and len(res.data["nodes"]) > 0) or ("edges" in res.data and len(res.data["edges"])) > 0:
                    query.cursors = res.cursors
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
        self.logger.debug(f"Ingest to lakehouse {state_id}")

        nodes = {}
        edges = {}
        if "nodes" in result:
            for node in result.get_nodes("nodes"):
                item = {
                    "space": node.space,
                    "instanceType": "node",
                    "externalId": node.external_id,
                    "version": node.version,
                    "lastUpdatedTime": node.last_updated_time,
                    "createdTime": node.created_time,
                }
                for view in node.properties.data:
                    propDict = node.properties.data[view]
                    item.update(propDict)

                table_name = f"{view.space}_{view.external_id}_{view.version}"
                if table_name not in nodes:
                    nodes[table_name] = [item]
                else:
                    nodes[table_name].append(item)
            

        if (len(nodes) > 0):
            token = self.azure_credential.get_token("https://storage.azure.com/.default")
            for table in nodes:
                abfss_path = f"{data_model_config.lakehouse_abfss_prefix}/Tables/{table}"
                logging.info(f"Writing {len(nodes[table])} to '{abfss_path}' table...")
                data = pa.Table.from_pylist(nodes[table])
                write_deltalake(abfss_path, data, mode="append", storage_options={"bearer_token": token.token, "use_fabric_endpoint": "true"})

        if (len(edges) > 0):
            token = self.azure_credential.get_token("https://storage.azure.com/.default")
            logging.info(f"Writing {len(edges)} to '{data_model_config.lakehouse_abfss_path_edges}' table...")
            data = pa.Table.from_pylist(edges, schema=self.schema)
            write_deltalake(data_model_config.lakehouse_abfss_path_edges, data, mode="append", storage_options={"bearer_token": token.token, "use_fabric_endpoint": "true"})

    