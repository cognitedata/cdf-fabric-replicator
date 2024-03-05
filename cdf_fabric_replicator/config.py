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
    externalId: str
    partitions: List[int]

@dataclass
class DataModelingConfig:
    space: str

@dataclass
class LakehouseConfig:
    abfss_path: str

@dataclass
class Config(BaseConfig):
    extractor: ExtractorConfig
    lakehouse: LakehouseConfig
    subscriptions: Optional[List[SubscriptionsConfig]]
    data_modeling: Optional[List[DataModelingConfig]]
