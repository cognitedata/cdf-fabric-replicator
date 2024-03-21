import pytest
from pytest_mock import mocker
import pandas as pd
from pandas import DataFrame
from azure.storage.filedatalake import DataLakeServiceClient
from cognite.client.data_classes import EventWrite, FileMetadata
from cdf_fabric_replicator.extractor import CdfFabricExtractor
from cdf_fabric_replicator.config import Config
from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.statestore import LocalStateStore, NoStateStore, RawStateStore
from cognite.extractorutils.configtools import (
    BaseConfig,
    CogniteConfig,
    FileSizeConfig,
    LoggingConfig,
    TimeIntervalConfig,
    load_yaml,
)
from cognite.client import CogniteClient

@pytest.fixture
def config_raw():
    yield """
    # Logging configuration
    logger:
        console:
            level: INFO

    # Cognite project to stream your datapoints from
    cognite:
        host: https://api.cognitedata.com
        project: unit_test_extractor

        idp-authentication:
            token-url: https://get-a-token.com/token
            client-id: abc123
            secret: def567
            scopes:
                - https://api.cognitedata.com/.default
        extraction-pipeline:
            external-id: test-fabric-extractor

    #Extractor config
    extractor:
        state-store:
            local:
                path: test_states.json
        subscription-batch-size: 10000
        ingest-batch-size: 100000
        poll-time: 5

    source:
        abfss_event_table_path: source_table_path
        abfss_directory: file_path
        abfss_raw_time_series_table_path: /table/path
        data_set_id: 123456

    destination:
        type: test_events
        time_series_prefix: test_ts_
    """
@pytest.fixture
def config(config_raw):
    yield load_yaml(config_raw, Config)


@pytest.fixture
def extractor(config):
    stop_event = CancellationToken()
    extractor = CdfFabricExtractor(stop_event=stop_event)
    extractor.config = config
    extractor.client = extractor.config.cognite.get_cognite_client("test_extractor")
    extractor.state_store = LocalStateStore(extractor.config.extractor.state_store.local.path)
    
    yield extractor


def test_parse_abfss_url(extractor):
    url = 'https://container@account.dfs.core.windows.net/path'
    result = extractor.parse_abfss_url(url)
    assert result == ('container', 'account', '/path')


def test_write_time_series_to_cdf(extractor, mocker):
    data = {
        'externalId': ['id1', 'id2', 'id1', 'id2'],
        'timestamp': ['2021-01-01', '2021-01-02', '2021-01-03', '2021-01-04'],
        'value': [1.0, 2.0, 3.0, 4.0]
    }
    df = pd.DataFrame(data)

    # Mocking dependencies
    mocker.patch.object(extractor.state_store, 'get_state', return_value=(None, ))
    mocker.patch.object(extractor.client.time_series.data, 'insert_dataframe')
    mocker.patch("cdf_fabric_replicator.extractor.CdfFabricExtractor.set_state", return_value=None)

    # Call the method with the DataFrame
    extractor.write_time_series_to_cdf(df)

    # Assertions to check if the methods were called with the correct arguments
    extractor.state_store.get_state.assert_any_call('/table/path-id1-state')
    extractor.state_store.get_state.assert_any_call('/table/path-id2-state')
    extractor.client.time_series.data.insert_dataframe.assert_any_call()
    extractor.set_state.assert_any_call('/table/path-id1-state', '2021-01-03')
    extractor.set_state.assert_any_call('/table/path-id2-state', '2021-01-04')
