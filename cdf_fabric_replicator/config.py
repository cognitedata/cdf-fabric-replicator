from dataclasses import dataclass
from typing import List, Optional

from cognite.extractorutils.configtools import BaseConfig, CogniteConfig, StateStoreConfig


@dataclass
class ExtractorConfig:
    state_store: StateStoreConfig = StateStoreConfig()
    subscription_batch_size: int = 10_000
    ingest_batch_size: int = 100_000
    poll_time: int = 5


@dataclass
class SubscriptionsConfig:
    external_id: str
    partitions: List[int]
    lakehouse_abfss_path: str

@dataclass
class DataModelingConfig:
    space: str
    lakehouse_abfss_path: str

@dataclass
class Config(BaseConfig):
    extractor: ExtractorConfig
    subscriptions: Optional[List[SubscriptionsConfig]]
    data_modeling: Optional[List[DataModelingConfig]]
