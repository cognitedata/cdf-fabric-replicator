import pytest
import pandas as pd
from cognite.client.data_classes import EventWrite
from cdf_fabric_replicator.extractor import CdfFabricExtractor
from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.statestore import LocalStateStore


@pytest.fixture(scope="session")
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
        "externalId": ["id1", "id2", "id1", "id2"],
        "timestamp": [
            "2022-02-07T16:01:27.001Z",
            "2022-02-07T16:03:47.000Z",
            "2022-02-07T16:03:52.000Z",
            "2022-02-07T16:04:12.000Z",
        ],
        "value": [1.0, 2.0, 3.0, 4.0],
    }
    df = pd.DataFrame(data)

    mocker.patch.object(extractor.state_store, 'get_state', return_value=(None, ))
    mocker.patch.object(extractor.client.time_series.data, 'insert_dataframe', return_value=None)
    mocker.patch("cdf_fabric_replicator.extractor.CdfFabricExtractor.set_state", return_value=None)

    extractor.write_time_series_to_cdf(df)

    for external_id in list(set(data["externalId"])):
        extractor.state_store.get_state.assert_any_call(f'/table/path-{external_id}-state')
        extractor.set_state.assert_any_call(f"/table/path-{external_id}-state", df[df["externalId"] == external_id]["timestamp"].max())

    assert(extractor.client.time_series.data.insert_dataframe.call_count == 2)


def test_write_time_series_to_cdf_filter_old_data_points(extractor, mocker):
    LAST_UPDATE_TIME = "2022-02-07T16:03:47.000Z"

    data = {
        "externalId": ["id1", "id1", "id1", "id1"],
        "timestamp": [
            "2022-02-07T16:01:27.001Z",
            LAST_UPDATE_TIME,
            "2022-02-07T16:03:52.000Z",
            "2022-02-07T16:04:12.000Z",
        ],
        "value": [1.0, 2.0, 3.0, 4.0],
    }
    df = pd.DataFrame(data)


    mocker.patch.object(extractor.state_store, "get_state", return_value=(LAST_UPDATE_TIME,))
    mocker.patch.object(extractor.client.time_series.data, "insert_dataframe", return_value=None)
    mocker.patch("cdf_fabric_replicator.extractor.CdfFabricExtractor.set_state", return_value=None)

    extractor.write_time_series_to_cdf(df)

    expected_update_list = df[df["timestamp"] > LAST_UPDATE_TIME]

    for external_id in list(set(data["externalId"])):
        extractor.state_store.get_state.assert_any_call(f'/table/path-{external_id}-state')
        extractor.set_state.assert_any_call(f"/table/path-{external_id}-state", df[df["externalId"] == external_id]["timestamp"].max())

    assert extractor.client.time_series.data.insert_dataframe.call_count == 1
    assert len(extractor.client.time_series.data.insert_dataframe.call_args_list[0]) == len(expected_update_list)


def test_write_time_series_to_cdf_no_new_data_points(extractor, mocker):
    LAST_UPDATE_TIME = "2022-02-07T16:04:12.000Z"

    data = {
        "externalId": ["id1", "id1", "id1", "id1"],
        "timestamp": [
            "2022-02-07T16:01:27.001Z",
            "2022-02-07T16:03:47.000Z",
            "2022-02-07T16:03:52.000Z",
            LAST_UPDATE_TIME,
        ],
        "value": [1.0, 2.0, 3.0, 4.0],
    }
    df = pd.DataFrame(data)

    mocker.patch.object(extractor.state_store, "get_state", return_value=(LAST_UPDATE_TIME,))
    mocker.patch.object(extractor.client.time_series.data, "insert_dataframe", return_value=None)
    mocker.patch("cdf_fabric_replicator.extractor.CdfFabricExtractor.set_state", return_value=None)

    extractor.write_time_series_to_cdf(df)

    for external_id in list(set(data["externalId"])):
        extractor.state_store.get_state.assert_any_call(f'/table/path-{external_id}-state')

    extractor.client.time_series.data.insert_dataframe.assert_not_called()
    extractor.set_state.assert_not_called()


def test_write_event_data_to_cdf(extractor, mocker):
    FILE_PATH = "test_file_path"
    TOKEN = "test_token"
    STATE_ID = "/table/path-state"

    mocker.patch.object(extractor, 'convert_lakehouse_data_to_df', return_value=pd.DataFrame())
    mocker.patch.object(extractor.state_store, 'get_state', return_value=(None, ))
    mocker.patch.object(extractor, 'get_events', return_value=[EventWrite()])
    mocker.patch.object(extractor.client.events, 'upsert', return_value=None)
    mocker.patch.object(extractor, 'run_extraction_pipeline', return_value=None)
    mocker.patch.object(extractor, 'set_state', return_value=None)

    extractor.write_event_data_to_cdf(FILE_PATH, TOKEN, STATE_ID)

    extractor.convert_lakehouse_data_to_df.assert_called_once_with(FILE_PATH, TOKEN)
    extractor.state_store.get_state.assert_called_once_with(STATE_ID)
    extractor.client.events.upsert.assert_called_once_with([EventWrite()])
    extractor.run_extraction_pipeline.assert_called_once_with(status="success")
    extractor.set_state.assert_called_once_with(STATE_ID, str(len(pd.DataFrame())))
