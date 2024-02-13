import json
import logging
import threading
import time
import datetime
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Any, Dict, List, Union

from azure.eventhub import EventData, EventHubProducerClient
from azure.eventhub.exceptions import EventHubError
from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries
from cognite.client.data_classes.datapoints_subscriptions import (
    DataDeletion,
    DatapointSubscriptionBatch,
    DatapointsUpdate,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.base import Extractor
from cognite.extractorutils.configtools import CogniteConfig
from cognite.extractorutils.metrics import BaseMetrics
from cognite.extractorutils.statestore import AbstractStateStore
from cognite.extractorutils.util import retry

from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config, LakehouseConfig
from cdf_fabric_replicator.metrics import Metrics


class TimeSeriesReplicator(Extractor):
    def __init__(self, metrics: Metrics, stop_event: Event) -> None:
        super().__init__(
            name="cdf_fabric_replicator",
            description="CDF Fabric Replicator",
            config_class=Config,
            metrics=metrics,
            use_default_state_store=False,
            version=__version__,
            cancellation_token=stop_event,
        )
        self.metrics: Metrics
        self.stop_event = stop_event
        self.endpoint_source_map: Dict[str, Any] = {}
        self.errors: List[str] = []
        self.update_queue: List[DatapointsUpdate] = []

        self.destination_cognite_client: Dict[str, CogniteClient] = {}
        self.eventhub_producer: Dict[str, EventHubProducerClient] = {}

    def run(self) -> None:
        # init/connect to destination
        self.state_store.initialize()

        for subscription in self.config.subscriptions:
            logging.info(
                f"{self.cognite_client.time_series.subscriptions.retrieve(external_id=subscription.externalId)}"
            )

        while not self.stop_event.is_set():
            start_time = time.time()  # Get the current time in seconds

            self.process_subscriptions()

            end_time = time.time()  # Get the time after function execution
            elapsed_time = end_time - start_time
            sleep_time = max(self.config.extractor.poll_time - elapsed_time, 0)  # 900s = 15min
            if sleep_time > 0:
                time.sleep(sleep_time)

    def process_subscriptions(self) -> None:
        for subscription in self.config.subscriptions:
            for partition in subscription.partitions:
                with ThreadPoolExecutor() as executor:
                    future = executor.submit(self.process_partition, subscription.externalId, partition)
                    logging.info(future.result())

    def process_partition(self, external_id: str, partition: int) -> str:
        state_id = f"{external_id}_{partition}"
        cursor = self.state_store.get_state(external_id=state_id)[1]
        logging.debug(f"{threading.get_native_id()} / {threading.get_ident()}: State for {state_id} is {cursor}")

        for update_batch in self.cognite_client.time_series.subscriptions.iterate_data(
            external_id=external_id,
            partition=partition,
            cursor=cursor,
            limit=self.config.extractor.subscription_batch_size,
        ):
            if update_batch.has_next:
                self.send_to_lakehouse(lakehouse=self.config.lakehouse, update_batch=update_batch, state_id=state_id, send_now=False)
            else:
                self.send_to_lakehouse(lakehouse=self.config.lakehouse, update_batch=update_batch, state_id=state_id, send_now=True)

            if not update_batch.has_next:
                return f"{state_id} no more data at {update_batch.cursor}"

        return "No new data"

    def send_to_lakehouse(
        self,
        lakehouse: LakehouseConfig,
        update_batch: DatapointSubscriptionBatch,
        state_id: str,
        send_now: bool = False,
    ) -> None:
        self.update_queue = self.update_queue + update_batch.updates
        logging.debug(f"update_queue length: {len(self.update_queue)}")

        if len(self.update_queue) > self.config.extractor.ingest_batch_size or send_now:
            self.logger.info(f"Ingest to queue of length {len(self.update_queue)} to lakehouse")
            self.send_to_lakehouse_table(table=lakehouse.lakehouse_table_name, updates=self.update_queue)

            self.extractor.state_store.set_state(external_id=state_id, high=update_batch.cursor)
            self.extractor.state_store.synchronize()
            self.update_queue = []

    
    def send_to_lakehouse_table(self, table, updates: List[DatapointsUpdate]) -> None:
        rows = []

        for update in updates:
            for i in range(0, len(update.upserts.timestamp)):
                rows.append( 
                    (update.upserts.external_id, 
                    datetime.datetime.fromtimestamp(update.upserts.timestamp[i]/1000), 
                    update.upserts.value[i]) )

        if (len(rows) > 0):    
            df = spark.createDataFrame(data=rows, schema = ["externalId", "timestamp", "value"])
            self.logger.info (f"writing {df.count()} rows to '{table}' table...")
            df.write.mode("append").format("delta").saveAsTable(table)
            self.logger.info ("done.")


