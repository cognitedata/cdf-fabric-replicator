import logging
import threading
import time
import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

from azure.identity import DefaultAzureCredential
from deltalake.writer import write_deltalake
import pandas as pd
<<<<<<< HEAD
=======
import numpy as np
>>>>>>> e72fbbbb80b7bf234db5b08db5c1619f786d2c11

from cognite.client.data_classes import ExtractionPipelineRunWrite
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointSubscriptionBatch,
    DatapointsUpdate,
)
from cognite.extractorutils.base import Extractor
from cognite.extractorutils.base import CancellationToken


from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config, SubscriptionsConfig
from cdf_fabric_replicator.metrics import Metrics


class TimeSeriesReplicator(Extractor):
    def __init__(self, metrics: Metrics, stop_event: CancellationToken) -> None:
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
        # logged in credentials, either local user or managed identity
        self.azure_credential = DefaultAzureCredential()
        self.ts_cache = {}

    def run(self) -> None:
        # init/connect to destination
        if not self.config.subscriptions:
            logging.info("No time series subscriptions found in config")
            return
    
        self.state_store.initialize()

        for subscription in self.config.subscriptions:
            logging.info(
                f"{self.cognite_client.time_series.subscriptions.retrieve(external_id=subscription.external_id)}"
            )

        while not self.stop_event.is_set():
            start_time = time.time()  # Get the current time in seconds

            self.process_subscriptions()
            self.cognite_client.extraction_pipelines.runs.create(ExtractionPipelineRunWrite(status="success", extpipe_external_id=self.config.cognite.extraction_pipeline.external_id))            
            end_time = time.time()  # Get the time after function execution
            elapsed_time = end_time - start_time
            sleep_time = max(self.config.extractor.poll_time - elapsed_time, 0)  # 900s = 15min
            if sleep_time > 0:
                time.sleep(sleep_time)

    def process_subscriptions(self) -> None:
        for subscription in self.config.subscriptions:
            for partition in subscription.partitions:
                with ThreadPoolExecutor() as executor:
                    future = executor.submit(self.process_partition, subscription, partition)
                    logging.debug(future.result())

    def process_partition(self, subscription: SubscriptionsConfig, partition: int) -> str:
        state_id = f"{subscription.external_id}_{partition}"
        cursor = self.state_store.get_state(external_id=state_id)[1]
        logging.debug(f"{threading.get_native_id()} / {threading.get_ident()}: State for {state_id} is {cursor}")

        for update_batch in self.cognite_client.time_series.subscriptions.iterate_data(
            external_id=subscription.external_id,
            partition=partition,
            cursor=cursor,
            limit=self.config.extractor.subscription_batch_size,
        ):
            if update_batch.has_next:
                self.send_to_lakehouse(subscription=subscription, update_batch=update_batch, state_id=state_id, send_now=False)
            else:
                self.send_to_lakehouse(subscription=subscription, update_batch=update_batch, state_id=state_id, send_now=True)

            if not update_batch.has_next:
                return f"{state_id} no more data at {update_batch.cursor}"

        return "No new data"

    def send_to_lakehouse(
        self,
        subscription: SubscriptionsConfig,
        update_batch: DatapointSubscriptionBatch,
        state_id: str,
        send_now: bool = False,
    ) -> None:
        self.update_queue = self.update_queue + update_batch.updates
        logging.debug(f"update_queue length: {len(self.update_queue)}")

        if len(self.update_queue) > self.config.extractor.ingest_batch_size or send_now:
            self.send_data_point_to_lakehouse_table(subscription=subscription, updates=self.update_queue)

            self.state_store.set_state(external_id=state_id, high=update_batch.cursor)
            self.state_store.synchronize()
            self.update_queue = []


    def send_data_point_to_lakehouse_table(self, subscription: SubscriptionsConfig, updates: List[DatapointsUpdate]) -> None:
        for update in updates:
            if update.upserts.external_id not in self.ts_cache:
                self.send_time_series_to_lakehouse_table(subscription, update)
        
        df = self.convert_updates_to_pandasdf(updates)
        if df is not None:
            
            logging.info (f"writing {df.shape[0]} rows to '{subscription.lakehouse_abfss_path_dps}' table...")
           
            self.write_pd_to_deltalake(subscription.lakehouse_abfss_path_dps, df)
            logging.info ("done.")
            
    def send_time_series_to_lakehouse_table(self, subscription: SubscriptionsConfig, update: DatapointsUpdate) -> None:
        ts=self.cognite_client.time_series.retrieve(external_id=update.upserts.external_id)
        print("timeseries: ", ts)
        asset = self.cognite_client.assets.retrieve(id=ts.asset_id) if ts.asset_id is not None else None
        print("asset_id: ", asset)
        asset_xid = asset.external_id if asset is not None else ""
        metadata = ts.metadata if len(ts.metadata) > 0 else {"source": "cdf_fabric_replicator"}
        df = pd.DataFrame(np.array([[ts.external_id, ts.name, ts.description if ts.description else "" , ts.is_string, ts.is_step, ts.unit, metadata, asset_xid]]), columns=["externalId", "name", "description", "isString", "isStep", "unit", "metadata", "assetExternalId"])
        df = df.dropna()
        self.logger.info (f"Writing {ts.external_id} to '{subscription.lakehouse_abfss_path_ts}' table...")
        if not df.empty:
            self.write_pd_to_deltalake(subscription.lakehouse_abfss_path_ts, df)
        self.ts_cache[update.upserts.external_id] = 1


    def convert_updates_to_pandasdf(self, updates: List[DatapointsUpdate]) -> pd.DataFrame:
        rows = []

        for update in updates:
            for i in range(0, len(update.upserts.timestamp)):
                rows.append( 
                    (update.upserts.external_id, 
                    datetime.datetime.fromtimestamp(update.upserts.timestamp[i]/1000), 
                    update.upserts.value[i]) )
        if len(rows) == 0:
            logging.info ("No data in updates list.")
            return None
        return pd.DataFrame(data=rows, columns=["externalId", "timestamp", "value"])


    def write_pd_to_deltalake(self, table: str, df: pd.DataFrame, mode="append") -> None:
        token = self.get_token()

        write_deltalake(table, df, mode=mode,engine="rust", schema_mode="merge", storage_options={"bearer_token": token, "use_fabric_endpoint": "true"})
        return None
    
    def get_token(self) -> str:
        return self.azure_credential.get_token("https://storage.azure.com/.default").token


