from dataclasses import dataclass
from typing import List, Optional

from cognite.extractorutils.configtools import BaseConfig, StateStoreConfig


@dataclass
class ExtractorConfig:
    state_store: StateStoreConfig
    subscription_batch_size: int = 10_000
    ingest_batch_size: int = 100_000
    fabric_ingest_batch_size: int = 1_000
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
class EventConfig:
    lakehouse_abfss_path_events: str
    dataset_external_id: str
    batch_size: int = 1000


@dataclass
class RawConfig:
    table_name: str
    db_name: str
    raw_path: str
    key_fields: Optional[List[str]]
    incremental_field: Optional[str]
    md5_key: bool = False


@dataclass
class RawConfigSource:
    table_name: str
    db_name: str
    lakehouse_abfss_path_raw: str


@dataclass
class SourceConfig:
    abfss_prefix: str
    data_set_id: str
    event_path: Optional[str] = None
    event_path_incremental_field: Optional[str] = None
    raw_time_series_path: Optional[str] = None
    read_batch_size: int = 1000
    file_path: Optional[str] = None
    raw_tables: Optional[List[RawConfig]] = None


@dataclass
class DestinationConfig:
    time_series_prefix: Optional[str] = None


@dataclass
class Config(BaseConfig):
    extractor: ExtractorConfig
    source: Optional[SourceConfig]
    destination: Optional[DestinationConfig]
    subscriptions: Optional[List[SubscriptionsConfig]]
    data_modeling: Optional[List[DataModelingConfig]]
    event: Optional[EventConfig]
    raw_tables: Optional[List[RawConfigSource]]
