from dataclasses import dataclass
from typing import Optional

from cognite.extractorutils.configtools import BaseConfig


@dataclass
class SourceConfig:
    data_set_id: int
    abfss_path: Optional[str] = None
    abfss_directory: Optional[str] = None


@dataclass
class DestinationConfig:
    type: str


@dataclass
class Config(BaseConfig):
    source: SourceConfig
    destination: DestinationConfig
