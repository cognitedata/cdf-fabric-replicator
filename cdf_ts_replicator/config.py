from dataclasses import dataclass
from typing import List, Union

from cognite.extractorutils.configtools import BaseConfig, CogniteConfig, StateStoreConfig


@dataclass
class ExtractorConfig:
    state_store: StateStoreConfig = StateStoreConfig()
    subscription_batch_size: int = 10_000
    ingest_batch_size: int = 100_000
    poll_time: int = 5


@dataclass
class SubscriptionsConfig:
    externalId: str
    partitions: List[int]


@dataclass
class EventHubConfig:
    connection_string: str
    eventhub_name: str
    use_jsonl: bool = True
    jsonl_batch_size: int = 100
    event_hub_batch_size: int = 262144


@dataclass
class Config(BaseConfig):
    extractor: ExtractorConfig
    destinations: List[Union[CogniteConfig, EventHubConfig]]
    subscriptions: List[SubscriptionsConfig]
