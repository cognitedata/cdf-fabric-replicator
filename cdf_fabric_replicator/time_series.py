import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from azure.identity import DefaultAzureCredential
from deltalake.exceptions import DeltaError, TableNotFoundError
from deltalake.writer import DeltaTable, write_deltalake

from cdf_fabric_replicator import __version__ as fabric_replicator_version
from cdf_fabric_replicator import subscription as sub
from cdf_fabric_replicator.config import Config, SubscriptionsConfig
from cdf_fabric_replicator.metrics import Metrics
from cognite.client.data_classes import ExtractionPipelineRunWrite, TimeSeries
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointSubscriptionBatch,
    DatapointsUpdate,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.base import CancellationToken, Extractor


class TimeSeriesReplicator(Extractor):
    def __init__(
        self,
        metrics: Metrics,
        stop_event: CancellationToken,
        override_config_path: Optional[str] = None,
    ) -> None:
        super().__init__(
            name="cdf_fabric_replicator_ts",
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
        self.update_queue: List[DatapointsUpdate] = []
        # logged in credentials, either local user or managed identity
        self.azure_credential = DefaultAzureCredential()
        self.ts_cache: Dict[str, int] = {}
        self.logger = logging.getLogger(self.name)

    def run(self) -> None:
        # init/connect to destination
        if not self.config.subscriptions or len(self.config.subscriptions) == 0:
            self.logger.info("No time series subscriptions found in config")
            return

        self.state_store.initialize()

        self.logger.debug(f"Current Subscription Config: {self.config.subscriptions}")

        sub.autocreate_subscription(
            self.config.subscriptions, self.cognite_client, self.name, self.logger
        )

        for subscription in self.config.subscriptions:
            try:
                self.logger.info(
                    f"{self.cognite_client.time_series.subscriptions.retrieve(external_id=subscription.external_id)}"
                )
            except CogniteAPIError as e:
                self.logger.error(
                    f"Error retrieving subscription {subscription.external_id}: {e}"
                )
                raise e

        while not self.stop_event.is_set():
            start_time = time.time()  # Get the current time in seconds

            self.process_subscriptions()
            end_time = time.time()  # Get the time after function execution
            elapsed_time = end_time - start_time
            sleep_time = max(
                self.config.extractor.poll_time - elapsed_time, 0
            )  # 900s = 15min
            if sleep_time > 0:
                self.logger.debug(f"Sleep for {sleep_time} seconds")
                self.stop_event.wait(sleep_time)

        self.logger.info("Stop event set. Exiting...")

    def process_subscriptions(self) -> None:
        for subscription in self.config.subscriptions:
            for partition in subscription.partitions:
                self.logger.debug(
                    f"Processing partition {partition} for subscription {subscription.external_id}"
                )
                with ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        self.process_partition, subscription, partition
                    )
                    self.logger.debug(future.result())

    def process_partition(
        self, subscription: SubscriptionsConfig, partition: int
    ) -> str:
        state_id = f"{subscription.external_id}_{partition}"
        raw_cursor = self.state_store.get_state(external_id=state_id)[1]
        cursor = str(raw_cursor) if raw_cursor is not None else None

        self.logger.debug(
            f"{threading.get_native_id()} / {threading.get_ident()}: State for {state_id} is {cursor}"
        )

        try:
            for (
                update_batch
            ) in self.cognite_client.time_series.subscriptions.iterate_data(
                external_id=subscription.external_id,
                partition=partition,
                cursor=cursor,
                limit=self.config.extractor.subscription_batch_size,
            ):
                if update_batch.has_next:
                    self.send_to_lakehouse(
                        subscription=subscription,
                        update_batch=update_batch,
                        state_id=state_id,
                        send_now=False,
                    )
                else:
                    self.send_to_lakehouse(
                        subscription=subscription,
                        update_batch=update_batch,
                        state_id=state_id,
                        send_now=True,
                    )
                self.cognite_client.extraction_pipelines.runs.create(
                    ExtractionPipelineRunWrite(
                        status="success",
                        extpipe_external_id=self.config.cognite.extraction_pipeline.external_id,
                    )
                )

                if not update_batch.has_next:
                    return f"{state_id} no more data at {update_batch.cursor}"
        except CogniteAPIError as e:
            self.logger.error(f"Error iterating partition {partition}: {e}")
            raise e

        return "No new data"

    def send_to_lakehouse(
        self,
        subscription: SubscriptionsConfig,
        update_batch: DatapointSubscriptionBatch,
        state_id: str,
        send_now: bool = False,
    ) -> None:
        self.update_queue = self.update_queue + update_batch.updates
        self.logger.debug(f"update_queue length: {len(self.update_queue)}")

        if len(self.update_queue) > self.config.extractor.ingest_batch_size or send_now:
            self.send_data_point_to_lakehouse_table(
                subscription=subscription, updates=self.update_queue
            )

            self.state_store.set_state(external_id=state_id, high=update_batch.cursor)
            self.state_store.synchronize()
            self.update_queue = []

    def send_data_point_to_lakehouse_table(
        self, subscription: SubscriptionsConfig, updates: List[DatapointsUpdate]
    ) -> None:
        for update in updates:
            if update.upserts.external_id not in self.ts_cache:
                self.send_time_series_to_lakehouse_table(subscription, update)

        df = self.convert_updates_to_pandasdf(updates)
        if df is not None:
            self.logger.info(
                f"writing {df.shape[0]} rows to '{subscription.lakehouse_abfss_path_dps}' table..."
            )

            self.write_pd_to_deltalake(subscription.lakehouse_abfss_path_dps, df)
            self.logger.info("done.")

    def send_time_series_to_lakehouse_table(
        self, subscription: SubscriptionsConfig, update: DatapointsUpdate
    ) -> None:
        try:
            ts = self.cognite_client.time_series.retrieve(
                external_id=update.upserts.external_id
            )
            if isinstance(ts, TimeSeries):
                asset = (
                    self.cognite_client.assets.retrieve(id=ts.asset_id)
                    if ts.asset_id is not None
                    else None
                )
                asset_xid = asset.external_id if asset is not None else ""

                metadata = (
                    ts.metadata
                    if len(ts.metadata.keys()) > 0
                    else {"source": "cdf_fabric_replicator"}
                )
                # Convert metadata to JSON string to avoid schema conflicts
                metadata_json = json.dumps(metadata)

                df = pd.DataFrame(
                    [
                        {
                            "externalId": ts.external_id,
                            "name": ts.name,
                            "description": ts.description if ts.description else "",
                            "isString": ts.is_string,
                            "isStep": ts.is_step,
                            "unit": ts.unit,
                            "metadata": metadata_json,
                            "assetExternalId": asset_xid,
                        }
                    ]
                )
                df = df.dropna()
                self.logger.info(
                    f"Writing {ts.external_id} to '{subscription.lakehouse_abfss_path_ts}' table..."
                )
                if not df.empty:
                    self.write_pd_to_deltalake(subscription.lakehouse_abfss_path_ts, df)
                self.ts_cache[str(update.upserts.external_id)] = 1
            else:
                self.logger.error(
                    f"Could not retrieve time series {update.upserts.external_id}"
                )
        except CogniteAPIError as e:
            self.logger.error(
                f"Error retrieving time series {update.upserts.external_id}: {e}"
            )
            raise e

    def convert_updates_to_pandasdf(
        self, updates: List[DatapointsUpdate]
    ) -> pd.DataFrame:
        rows = []

        for update in updates:
            for i in range(0, len(update.upserts.timestamp)):
                rows.append(
                    (
                        update.upserts.external_id,
                        pd.to_datetime(
                            update.upserts.timestamp[i], unit="ms", utc=True
                        ),
                        update.upserts.value[i],  # type: ignore
                    )
                )
        if len(rows) == 0:
            self.logger.info("No data in updates list.")
            return None
        return pd.DataFrame(data=rows, columns=["externalId", "timestamp", "value"])

    def write_or_merge_to_lakehouse_table(
        self,
        table: str,
        df: pd.DataFrame,
        mode: Literal["error", "append", "overwrite", "ignore"] = "append",
        storage_options: Dict[str, str] = {},
    ) -> None:
        try:
            dt = DeltaTable(table, storage_options=storage_options)
            # Build predicate based on available columns
            # For time series metadata: only use externalId
            # For data points: use both externalId and timestamp
            if "timestamp" in df.columns:
                predicate = "s.externalId = t.externalId AND s.timestamp = t.timestamp"
            else:
                predicate = "s.externalId = t.externalId"

            (
                dt.merge(
                    source=df,
                    predicate=predicate,
                    source_alias="s",
                    target_alias="t",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        except TableNotFoundError:
            write_deltalake(
                table_or_uri=table,
                data=df,
                mode=mode,
                engine="rust",
                schema_mode="merge",
                storage_options=storage_options,
            )

    def write_pd_to_deltalake(
        self,
        table: str,
        df: pd.DataFrame,
        mode: Literal["error", "append", "overwrite", "ignore"] = "append",
    ) -> None:
        storage_options = {
            "bearer_token": self.get_token(),
            "use_fabric_endpoint": str(
                self.config.extractor.use_fabric_endpoint
            ).lower(),
        }

        try:
            self.write_or_merge_to_lakehouse_table(
                table=table,
                df=df,
                mode=mode,
                storage_options=storage_options,
            )
        except DeltaError as e:
            self.logger.error(f"Error writing to {table}: {e}")
            raise e

        return None

    def get_token(self) -> str:
        return self.azure_credential.get_token(
            "https://storage.azure.com/.default"
        ).token
