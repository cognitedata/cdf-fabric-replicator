import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa
from azure.identity import DefaultAzureCredential
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError

from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config
from cdf_fabric_replicator.metrics import Metrics
from cognite.client.data_classes import Event, EventList
from cognite.extractorutils.base import CancellationToken, Extractor


class EventsReplicator(Extractor):
    def __init__(
        self,
        metrics: Metrics,
        stop_event: CancellationToken,
        override_config_path: Optional[str] = None,
    ) -> None:
        super().__init__(
            name="cdf_fabric_replicator_events",
            description="CDF Fabric Replicator",
            config_class=Config,
            metrics=metrics,
            use_default_state_store=False,
            version=__version__,
            cancellation_token=stop_event,
            config_file_path=override_config_path,
        )
        self.azure_credential = DefaultAzureCredential()
        self.event_state_key = "event_state"
        self.stop_event = stop_event
        self.logger = logging.getLogger(self.name)

    def run(self) -> None:
        self.logger.info("Run Called for Events Extractor...")
        # init/connect to destination
        self.state_store.initialize()

        self.logger.debug(f"Current Event Config: {self.config.event}")

        if self.config.event is None:
            self.logger.warning("No event config found in config")
            return

        while not self.stop_event.is_set():
            start_time = time.time()  # Get the current time in seconds

            self.process_events()
            end_time = time.time()  # Get the time after function execution
            elapsed_time = end_time - start_time
            sleep_time = max(self.config.extractor.poll_time - elapsed_time, 0)

            if sleep_time > 0:
                self.logger.debug(f"Sleep for {sleep_time} seconds")
                self.stop_event.wait(sleep_time)

        self.logger.info("Stop event set. Exiting...")

    def process_events(self) -> None:
        limit = self.config.event.batch_size
        dataset_external_id = self.config.event.dataset_external_id

        last_created_time = self.get_event_state(self.event_state_key)

        if last_created_time is None:
            last_created_time = 0
            self.logger.debug("No last created time found.")
        else:
            self.logger.debug(f"Last created time: {datetime.fromtimestamp(last_created_time / 1000).isoformat()}")

        optimize = False
        for event_list in self.get_events(limit, last_created_time, dataset_external_id):
            events_dict = event_list.dump()
            if len(events_dict) > 0:
                if isinstance(events_dict, dict):
                    events_dict = [events_dict]

                for event in events_dict:
                    event["metadata"] = json.dumps(event["metadata"])

                try:
                    self.write_events_to_lakehouse_tables(events_dict, self.config.event.lakehouse_abfss_path_events)
                    optimize = True
                except DeltaError as e:
                    self.logger.error(f"Error writing events to lakehouse tables: {e}")
                    raise e
                last_event = events_dict[-1]
                self.set_event_state(self.event_state_key, last_event["createdTime"])
            else:
                self.logger.info("No events found in current batch.")
        if optimize:
            self.optimize_table(self.config.event.lakehouse_abfss_path_events)

    def get_events(
        self, limit: int, last_created_time: int, dataset_external_id: str
    ) -> Iterator[Event] | Iterator[EventList]:
        # only pull events that created after last_created_time (hence the +1); assuming no other
        # events are created at the same time
        self.logger.debug(f"Getting events with limit: {limit}, last_created_time: {last_created_time}")
        return self.cognite_client.events(
            chunk_size=limit,
            created_time={"min": last_created_time + 1},
            sort=("createdTime", "asc"),
            data_set_external_ids=[dataset_external_id],
        )

    def get_event_state(self, event_state_key: str) -> int | None:
        state = self.state_store.get_state(external_id=event_state_key)
        if isinstance(state, list):
            state = state[0]
        return int(state[1]) if state is not None and state[1] is not None else None

    def set_event_state(self, event_state_key: str, created_time: int) -> None:
        self.state_store.set_state(external_id=event_state_key, high=created_time)
        self.state_store.synchronize()
        self.logger.debug(f"Event state set: {created_time}")

    def write_or_merge_to_lakehouse_table(
        self, abfss_path: str, storage_options: Dict[str, str], data: pa.Table
    ) -> None:
        write_deltalake(
            abfss_path,
            data,
            mode="append",
            engine="rust",
            schema_mode="merge",
            storage_options=storage_options,
        )

    def optimize_table(self, abfss_path: str) -> None:
        token = self.azure_credential.get_token("https://storage.azure.com/.default")
        storage_options = {
            "bearer_token": token.token,
            "timeout": "1800s",
            # "use_fabric_endpoint": "true",
        }

        dt = DeltaTable(abfss_path, storage_options=storage_options)
        try:
            self.logger.debug(f"Compacting table: {abfss_path}")
            self.logger.debug(dt.optimize.compact(target_size=256 * 1024 * 1024))
            self.logger.debug(f"Vacuuming table: {abfss_path}")
            self.logger.debug(dt.vacuum(retention_hours=1, enforce_retention_duration=False))
            dt.create_checkpoint()
            self.logger.debug("Done optimizing table.")
        except DeltaError as e:
            self.logger.error(f"Error optimizing table: {e}")
            raise e

    def write_events_to_lakehouse_tables(self, events: List[Dict[str, Any]], abfss_path: str) -> None:
        token = self.azure_credential.get_token("https://storage.azure.com/.default")

        self.logger.info(f"Writing {len(events)} events to '{abfss_path}' table...")
        data = pa.Table.from_pylist(events)
        storage_options = {
            "bearer_token": token.token,
            # "use_fabric_endpoint": "true",
        }

        try:
            self.write_or_merge_to_lakehouse_table(abfss_path, storage_options, data)
        except DeltaError as e:
            self.logger.error(f"Error writing events to lakehouse tables: {e}")
            raise e

        self.logger.info("done.")
