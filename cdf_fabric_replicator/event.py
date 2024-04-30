import logging
import time
from typing import Iterator, List, Dict, Any
from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.base import Extractor
from azure.identity import DefaultAzureCredential
from deltalake import write_deltalake
from deltalake.exceptions import DeltaError
import pyarrow as pa
from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config
from cdf_fabric_replicator.metrics import Metrics
from cognite.client.data_classes import EventList, Event
from datetime import datetime


class EventsReplicator(Extractor):
    def __init__(self, metrics: Metrics, stop_event: CancellationToken) -> None:
        super().__init__(
            name="cdf_fabric_replicator_events",
            description="CDF Fabric Replicator",
            config_class=Config,
            metrics=metrics,
            use_default_state_store=False,
            version=__version__,
            cancellation_token=stop_event,
        )
        self.azure_credential = DefaultAzureCredential()
        self.event_state_key = "event_state"
        self.stop_event = stop_event
        self.logger = logging.getLogger(self.name)

    def run(self) -> None:
        self.logger.info("Run Called for Events Extractor...")
        # init/connect to destination
        self.state_store.initialize()

        self.logger.debug("Current Event Config: %s", self.config.event)

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
                logging.debug(f"Sleep for {sleep_time} seconds")
                time.sleep(sleep_time)

        self.logger.info("Stop event set. Exiting...")

    def process_events(self) -> None:
        limit = self.config.event.batch_size
        last_created_time = self.get_event_state(self.event_state_key)

        if last_created_time is None:
            last_created_time = 0
            self.logger.debug("No last created time found.")
        else:
            self.logger.debug(
                "Last created time: %s",
                datetime.fromtimestamp(last_created_time / 1000).isoformat(),
            )

        for event_list in self.get_events(limit, last_created_time):
            events_dict = event_list.dump()
            if len(events_dict) > 0:
                if isinstance(events_dict, dict):
                    events_dict = [events_dict]
                try:
                    self.write_events_to_lakehouse_tables(
                        events_dict, self.config.event.lakehouse_abfss_path_events
                    )
                except DeltaError as e:
                    self.logger.error("Error writing events to lakehouse tables: %s", e)
                    raise e
                last_event = events_dict[-1]
                self.set_event_state(self.event_state_key, last_event["createdTime"])
            else:
                self.logger.info("No events found in current batch.")

    def get_events(
        self, limit: int, last_created_time: int
    ) -> Iterator[Event] | Iterator[EventList]:
        # only pull events that created after last_created_time (hence the +1); assuming no other events are created at the same time
        self.logger.debug(
            "Getting events with limit: %s, last_created_time: %s",
            limit,
            last_created_time,
        )
        return self.cognite_client.events(
            chunk_size=limit,
            created_time={"min": last_created_time + 1},
            sort=("createdTime", "asc"),
        )

    def get_event_state(self, event_state_key: str) -> int | None:
        state = self.state_store.get_state(external_id=event_state_key)
        if isinstance(state, list):
            state = state[0]
        return int(state[1]) if state is not None and state[1] is not None else None

    def set_event_state(self, event_state_key: str, created_time: int) -> None:
        self.state_store.set_state(external_id=event_state_key, high=created_time)
        self.state_store.synchronize()
        self.logger.debug("Event state set: %s", created_time)

    def write_events_to_lakehouse_tables(
        self, events: List[Dict[str, Any]], abfss_path: str
    ) -> None:
        token = self.azure_credential.get_token("https://storage.azure.com/.default")

        self.logger.info(f"Writing {len(events)} to '{abfss_path}' table...")
        data = pa.Table.from_pylist(events)
        try:
            write_deltalake(
                abfss_path,
                data,
                mode="append",
                engine="rust",
                schema_mode="merge",
                storage_options={
                    "bearer_token": token.token,
                    "use_fabric_endpoint": "true",
                },
            )
        except DeltaError as e:
            self.logger.error("Error writing events to lakehouse tables: %s", e)
            raise e

        self.logger.info("done.")
