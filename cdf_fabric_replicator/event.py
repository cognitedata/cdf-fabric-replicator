import logging
import time

from typing import Any, Dict, List, Iterator
from cognite.extractorutils.base import CancellationToken

from cognite.extractorutils.base import Extractor

from azure.identity import DefaultAzureCredential
from deltalake import write_deltalake
import pyarrow as pa

from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config
from cdf_fabric_replicator.metrics import Metrics
from cognite.client.data_classes import EventList

class EventsReplicator(Extractor):
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
        # self.stop_event = stop_event
        self.endpoint_source_map: Dict[str, Any] = {}
        self.errors: List[str] = []
        self.azure_credential = DefaultAzureCredential()
        self.event_state_key = "event_state"

    def run(self) -> None:
        # init/connect to destination
        self.state_store.initialize()

        if self.config.event is None:
            logging.info("No event config found in config")
            return

        while True:  # not self.stop_event.is_set():
            start_time = time.time()  # Get the current time in seconds

            self.process_events()
            end_time = time.time()  # Get the time after function execution
            elapsed_time = end_time - start_time
            sleep_time = max(
                self.config.extractor.poll_time - elapsed_time, 0
            )  # 900s = 15min

            if sleep_time > 0:
                logging.debug(f"Sleep for {sleep_time} seconds")
                time.sleep(sleep_time)

    def process_events(self) -> None:
        limit = self.config.event.batch_size
        last_created_time = self.get_event_state(self.event_state_key)
        if last_created_time is None:
            last_created_time = 0

        for event_list in self.get_events(limit, last_created_time):
            event_list = event_list.dump()
            
            if len(event_list) == 0:
                break
            self.write_events_to_lakehouse_tables(event_list, self.config.event.lakehouse_abfss_prefix)
            last_event = event_list[-1]
            self.set_event_state(self.event_state_key, last_event["createdTime"])

    def get_events(self, limit: int, last_created_time: int) -> Iterator[EventList]:
        return self.cognite_client.events(chunk_size=limit, created_time={"min": last_created_time+1}, sort=("createdTime", "asc"))

    def get_event_state(self, event_state_key: str) -> int:
        return self.state_store.get_state(external_id=event_state_key)[1]
    
    def set_event_state(self, event_state_key: str, created_time: int) -> None:
        self.state_store.set_state(external_id=event_state_key, high = created_time)
        self.state_store.synchronize()

    def write_events_to_lakehouse_tables(
        self, events: List[Dict[str, Any]], abfss_prefix: str
    ) -> None:
        token = self.azure_credential.get_token("https://storage.azure.com/.default")
        
        abfss_path = f"{abfss_prefix}/Tables/events"
        logging.info(f"Writing {len(events)} to '{abfss_path}' table...")
        data = pa.Table.from_pylist(events)
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
        logging.info("done.")