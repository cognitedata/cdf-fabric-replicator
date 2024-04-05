from dataclasses import dataclass
from typing import List, Optional

from cognite.extractorutils.configtools import BaseConfig, StateStoreConfig


@dataclass
class ExtractorConfig:
    state_store: StateStoreConfig
    subscription_batch_size: int = 10_000
    ingest_batch_size: int = 100_000
    poll_time: int = 3600


@dataclass
class SubscriptionsConfig:
    external_id: str
    partitions: List[int]
    lakehouse_abfss_path_dps: str
    lakehouse_abfss_path_ts: str


@dataclass
class DataModelingConfig:
    space: str
    lakehouse_abfss_prefix: str


@dataclass
class SourceConfig:
    abfss_prefix: str
    data_set_id: str
    event_path: Optional[str] = None
    raw_time_series_path: Optional[str] = None
    file_path: Optional[str] = None


@dataclass
class DestinationConfig:
    type: str
    time_series_prefix: Optional[str] = None


@dataclass
class Config(BaseConfig):
    extractor: ExtractorConfig
    source: Optional[SourceConfig]
    destination: Optional[DestinationConfig]
    subscriptions: Optional[List[SubscriptionsConfig]]
    data_modeling: Optional[List[DataModelingConfig]]
