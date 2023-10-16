import json
import logging
import threading
import time
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

from cdf_ts_replicator import __version__
from cdf_ts_replicator.config import Config, EventHubConfig
from cdf_ts_replicator.metrics import Metrics


class TimeSeriesReplicator(Extractor):
    def __init__(self, metrics: Metrics, stop_event: Event) -> None:
        super().__init__(
            name="cdf_ts_replicator",
            description="CDF Timeseries Replicator",
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
            for destination in self.config.destinations:
                if update_batch.has_next:
                    self.submit_to_destination(destination, update_batch, state_id, False)
                else:
                    self.submit_to_destination(destination, update_batch, state_id, True)

            if not update_batch.has_next:
                return f"{state_id} no more data at {update_batch.cursor}"

        return "No new data"

    def submit_to_destination(
        self,
        destination: Union[CogniteConfig, EventHubConfig],
        update_batch: DatapointSubscriptionBatch,
        state_id: str,
        send_now: bool = False,
    ) -> None:
        self.update_queue = self.update_queue + update_batch.updates
        logging.debug(f"update_queue length: {len(self.update_queue)}")

        if len(self.update_queue) > self.config.extractor.ingest_batch_size or send_now:
            logging.info(f"Ingest to queue of length {len(self.update_queue)} to destinations")
            if type(destination) == EventHubConfig:
                self.send_to_eventhub(updates=self.update_queue, config=destination)
            elif type(destination) == CogniteConfig:
                self.send_to_cdf(updates=self.update_queue, config=destination)
            else:
                print("Unknown destination type")

            self.state_store.set_state(external_id=state_id, high=update_batch.cursor)
            self.update_queue = []

    def _get_producer(self, connection_string: str, eventhub_name: str) -> Union[EventHubProducerClient, None]:
        if connection_string == None or eventhub_name == None:
            return None

        return EventHubProducerClient.from_connection_string(conn_str=connection_string, eventhub_name=eventhub_name)

    def send_to_eventhub(self, updates: List[DatapointsUpdate], config: EventHubConfig) -> None:
        if not config.eventhub_name in self.eventhub_producer:  # handle multiple producers
            self.eventhub_producer[config.eventhub_name] = self._get_producer(
                config.connection_string, config.eventhub_name
            )

        producer = self.eventhub_producer[config.eventhub_name]

        if producer:
            try:
                event_data_batch = producer.create_batch(max_size_in_bytes=config.event_hub_batch_size)
                jsonLines = ""
                for update in updates:
                    for i in range(0, len(update.upserts.timestamp)):
                        try:
                            jsonData = json.dumps(
                                {
                                    "externalId": update.upserts.external_id,
                                    "timestamp": update.upserts.timestamp[i],
                                    "value": update.upserts.value[i],
                                },
                            )

                            if not config.use_jsonl:
                                event_data_batch.add(EventData(jsonData))
                            else:
                                jsonLines = jsonLines + f"\n{jsonData}"
                                if len(jsonLines.split("\n")) == config.jsonl_batch_size:
                                    event_data_batch.add(EventData(jsonLines))
                                    jsonLines = ""

                        except ValueError:
                            # EventDataBatch object reaches max_size.
                            logging.info("Sending batch to event hub")
                            producer.send_batch(event_data_batch)
                            event_data_batch = producer.create_batch(max_size_in_bytes=config.event_hub_batch_size)
                            if not config.use_jsonl:
                                event_data_batch.add(EventData(jsonData))
                            elif config.use_jsonl:
                                event_data_batch.add(EventData(jsonLines))
                                jsonLines = ""

                    for delete in update.deletes:
                        try:
                            jsonData = json.dumps(
                                {
                                    "externalId": update.time_series.external_id,
                                    "inclusive_begin": delete.inclusive_begin,
                                    "exclusive_end": delete.exclusive_end,
                                    "operation": "DELETE",
                                },
                            )

                            if not config.use_jsonl:
                                event_data_batch.add(EventData(jsonData))
                            else:
                                jsonLines = jsonLines + f"\n{jsonData}"
                                if len(jsonLines.split("\n")) == config.jsonl_batch_size:
                                    event_data_batch.add(EventData(jsonLines))
                                    jsonLines = ""

                        except ValueError:
                            # EventDataBatch object reaches max_size.
                            logging.info("Sending batch to event hub")
                            producer.send_batch(event_data_batch)
                            event_data_batch = producer.create_batch(max_size_in_bytes=config.event_hub_batch_size)
                            if not config.use_jsonl:
                                event_data_batch.add(EventData(jsonData))
                            elif config.use_jsonl:
                                event_data_batch.add(EventData(jsonLines))
                                jsonLines = ""

                event_data_batch.add(EventData(jsonLines))
                logging.info("Sending batch to event hub")
                producer.send_batch(event_data_batch)
            except EventHubError as eh_err:
                logging.warning("Sending error: ", eh_err)

    def send_to_cdf(self, updates: List[DatapointsUpdate], config: CogniteConfig) -> None:
        if not config.host + config.project in self.destination_cognite_client:  # todo handle multiple CDF destiations
            self.destination_cognite_client[config.host + config.project] = config.get_cognite_client(
                client_name="ts-replicator"
            )

        client = self.destination_cognite_client[config.host + config.project] = config.get_cognite_client(
            client_name="ts-replicator"
        )

        try:
            dps: Dict[str, list] = {}
            for update in updates:
                if len(update.upserts) > 0:
                    xid = config.external_id_prefix + update.upserts.external_id
                    if not xid in dps:
                        dps[xid] = []
                    for i in range(0, len(update.upserts.timestamp)):
                        dps[xid].append((update.upserts.timestamp[i], update.upserts.value[i]))
                if len(update.deletes) > 0:
                    xid = config.external_id_prefix + update.time_series.external_id
                    for delete in update.deletes:
                        client.time_series.data.delete_range(
                            external_id=xid, start=delete.inclusive_begin, end=delete.exclusive_end
                        )

            ingest_dps = [{"external_id": external_id, "datapoints": dps[external_id]} for external_id in dps]
            client.time_series.data.insert_multiple(ingest_dps)

        except CogniteAPIError as err:
            for update in updates:
                try:
                    client.time_series.create(
                        TimeSeries(external_id=update.upserts.external_id, name=update.upserts.external_id)
                    )
                except CogniteAPIError as err:
                    print(err)

            client.time_series.data.insert_multiple(ingest_dps)
