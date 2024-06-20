import logging
import time
from typing import Dict

from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.base import Extractor
from azure.identity import DefaultAzureCredential
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import DeltaError, TableNotFoundError
import pyarrow as pa
from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config
from cdf_fabric_replicator.metrics import Metrics
from cognite.client.data_classes import RowList
from datetime import datetime


class RawTableReplicator(Extractor):
    def __init__(self, metrics: Metrics, stop_event: CancellationToken) -> None:
        super().__init__(
            name="cdf_fabric_replicator_raw",
            description="CDF Fabric Replicator",
            config_class=Config,
            metrics=metrics,
            use_default_state_store=False,
            version=__version__,
            cancellation_token=stop_event,
        )
        self.azure_credential = DefaultAzureCredential()
        self.stop_event = stop_event
        self.logger = logging.getLogger(self.name)

    def run(self) -> None:
        self.logger.info("Run Called for Raw Extractor...")
        # init/connect to destination
        self.state_store.initialize()

        self.logger.debug(f"Current Raw Config: {self.config.raw_tables}")

        if self.config.raw_tables is None:
            self.logger.warning("No Raw config found in config")
            return

        while not self.stop_event.is_set():
            start_time = time.time()  # Get the current time in seconds

            self.process_raw_tables()
            end_time = time.time()  # Get the time after function execution
            elapsed_time = end_time - start_time
            sleep_time = max(self.config.extractor.poll_time - elapsed_time, 0)

            if sleep_time > 0:
                self.logger.debug(f"Sleep for {sleep_time} seconds")
                self.stop_event.wait(sleep_time)

        self.logger.info("Stop event set. Exiting...")

    def process_raw_tables(self) -> None:
        for raw_table in self.config.raw_tables:
            state_id = f"{raw_table.db_name}_{raw_table.table_name}_raw_state"
            last_updated_time = self.get_state(state_id)

            if last_updated_time is None:
                last_updated_time = 0
                self.logger.debug(
                    f"No last update time in state store with key {state_id}."
                )
            else:
                self.logger.debug(
                    f"Last updated time: {datetime.fromtimestamp(last_updated_time / 1000).isoformat()}"
                )

            feedRows = True
            while feedRows:
                rows = self.cognite_client.raw.rows.list(
                    db_name=raw_table.db_name,
                    table_name=raw_table.table_name,
                    min_last_updated_time=last_updated_time,
                    limit=self.config.extractor.fabric_ingest_batch_size,
                )
                if len(rows) > 0:
                    try:
                        self.write_rows_to_lakehouse_table(
                            rows, raw_table.lakehouse_abfss_path_raw
                        )
                        last_row = rows[-1]
                        self.set_state(state_id, last_row.last_updated_time)
                    except DeltaError as e:
                        self.logger.error(
                            f"Error writing raw rows to lakehouse tables: {e}"
                        )
                        raise e
                else:
                    feedRows = False

    def get_state(self, state_key: str) -> int | None:
        state = self.state_store.get_state(external_id=state_key)
        if isinstance(state, list):
            state = state[0]
        return int(state[0]) if state is not None and state[0] is not None else None

    def set_state(self, state_key: str, updated_time: int | None) -> None:
        if updated_time:
            self.state_store.set_state(external_id=state_key, low=updated_time)
            self.state_store.synchronize()
            self.logger.debug(f"State {state_key} set: {updated_time}")
        else:
            self.logger.debug(f"State {state_key} not set.")

    def write_rows_to_lakehouse_table(self, rows: RowList, abfss_path: str) -> None:
        token = self.azure_credential.get_token("https://storage.azure.com/.default")

        rows_dict = []
        for row in rows:
            row_dict = row.dump()["columns"]
            row_dict["key"] = row.key
            row_dict["last_updated_time"] = row.last_updated_time
            rows_dict.append(row_dict)
        if len(rows_dict) > 0:
            self.logger.info(f"Writing {len(rows)} rows to '{abfss_path}' table...")
            data = pa.Table.from_pylist(rows_dict)
            storage_options = {
                "bearer_token": token.token,
                "use_fabric_endpoint": "true",
            }

            try:
                self.write_or_merge_to_lakehouse_table(
                    abfss_path, storage_options, data
                )
            except DeltaError as e:
                self.logger.error(f"Error writing rows to lakehouse tables: {e}")
                raise e

            self.logger.info("Done.")

    def write_or_merge_to_lakehouse_table(
        self, abfss_path: str, storage_options: Dict[str, str], data: pa.Table
    ) -> None:
        try:
            dt = DeltaTable(
                abfss_path,
                storage_options=storage_options,
            )

            (
                dt.merge(
                    source=data,
                    predicate="s.key = t.key",
                    source_alias="s",
                    target_alias="t",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        except TableNotFoundError:
            write_deltalake(
                abfss_path,
                data,
                mode="append",
                engine="rust",
                schema_mode="merge",
                storage_options=storage_options,
            )
