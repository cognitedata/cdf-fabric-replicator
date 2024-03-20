import pytest
from pytest_mock import mocker
import pandas as pd
from pandas import DataFrame
from azure.storage.filedatalake import DataLakeServiceClient
from cognite.client.data_classes import EventWrite, FileMetadata
from cdf_fabric_replicator.extractor import CdfFabricExtractor
from cognite.extractorutils.base import CancellationToken


@pytest.fixture
def extractor():
    stop_event = CancellationToken()
    return CdfFabricExtractor(stop_event=stop_event)



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
    mocker.patch.object(extractor, 'set_state')
    mocker.patch.object(extractor.config.destination, 'test_ts_')
    mocker.patch.object(extractor.config.source.abfss_raw_time_series_table_path, '/table/path')

    # Call the method with the DataFrame
    extractor.write_time_series_to_cdf(df)

    # Assertions to check if the methods were called with the correct arguments
    extractor.state_store.get_state.assert_any_call('/table/path-id1-state')
    extractor.state_store.get_state.assert_any_call('/table/path-id2-state')
    extractor.client.time_series.data.insert_dataframe.assert_called_once()
    extractor.set_state.assert_any_call('/table/path-id1-state', '2021-01-03')
    extractor.set_state.assert_any_call('/table/path-id2-state', '2021-01-04')
