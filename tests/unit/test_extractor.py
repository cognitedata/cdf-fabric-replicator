import pytest
import pandas as pd
from cognite.client.data_classes import EventWrite
from cdf_fabric_replicator.extractor import CdfFabricExtractor
from cdf_fabric_replicator.config import Config
from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.statestore import LocalStateStore
from cognite.extractorutils.configtools import load_yaml

@pytest.fixture(scope="session")
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
@pytest.fixture(scope="session")
def config(config_raw):
    yield load_yaml(config_raw, Config)


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

    extractor.state_store.get_state.assert_any_call('/table/path-id1-state')
    extractor.state_store.get_state.assert_any_call('/table/path-id2-state')
    assert(extractor.client.time_series.data.insert_dataframe.call_count == 2)
    extractor.set_state.assert_any_call("/table/path-id1-state", df[df["externalId"] == "id1"]["timestamp"].max())
    extractor.set_state.assert_any_call("/table/path-id2-state", df[df["externalId"] == "id2"]["timestamp"].max())


def test_write_time_series_to_cdf_filter_old_data_points(extractor, mocker):
    data = {
        "externalId": ["id1", "id1", "id1", "id1"],
        "timestamp": [
            "2022-02-07T16:01:27.001Z",
            "2022-02-07T16:03:47.000Z",
            "2022-02-07T16:03:52.000Z",
            "2022-02-07T16:04:12.000Z",
        ],
        "value": [1.0, 2.0, 3.0, 4.0],
    }
    df = pd.DataFrame(data)

    last_update_time = "2022-02-07T16:03:47.000Z"

    mocker.patch.object(extractor.state_store, "get_state", return_value=(last_update_time,))
    mocker.patch.object(extractor.client.time_series.data, "insert_dataframe", return_value=None)
    mocker.patch("cdf_fabric_replicator.extractor.CdfFabricExtractor.set_state", return_value=None)

    extractor.write_time_series_to_cdf(df)

    expected_update_list = df[df["timestamp"] > last_update_time]

    extractor.state_store.get_state.assert_any_call("/table/path-id1-state")
    assert extractor.client.time_series.data.insert_dataframe.call_count == 1
    assert len(extractor.client.time_series.data.insert_dataframe.call_args_list[0]) == len(expected_update_list)
    extractor.set_state.assert_any_call("/table/path-id1-state", df[df["externalId"] == "id1"]["timestamp"].max())


def test_write_time_series_to_cdf_no_new_data_points(extractor, mocker):
    data = {
        "externalId": ["id1", "id1", "id1", "id1"],
        "timestamp": [
            "2022-02-07T16:01:27.001Z",
            "2022-02-07T16:03:47.000Z",
            "2022-02-07T16:03:52.000Z",
            "2022-02-07T16:04:12.000Z",
        ],
        "value": [1.0, 2.0, 3.0, 4.0],
    }
    df = pd.DataFrame(data)

    last_update_time = "2022-02-07T16:04:12.000Z"

    mocker.patch.object(extractor.state_store, "get_state", return_value=(last_update_time,))
    mocker.patch.object(extractor.client.time_series.data, "insert_dataframe", return_value=None)
    mocker.patch("cdf_fabric_replicator.extractor.CdfFabricExtractor.set_state", return_value=None)

    extractor.write_time_series_to_cdf(df)

    extractor.state_store.get_state.assert_any_call("/table/path-id1-state")
    extractor.client.time_series.data.insert_dataframe.assert_not_called()
    extractor.set_state.assert_not_called()


def test_write_event_data_to_cdf(extractor, mocker):
    file_path = "test_file_path"
    token = "test_token"
    state_id = "/table/path-state"

    mocker.patch.object(extractor, 'convert_lakehouse_data_to_df', return_value=pd.DataFrame())
    mocker.patch.object(extractor.state_store, 'get_state', return_value=(None, ))
    mocker.patch.object(extractor, 'get_events', return_value=[EventWrite()])
    mocker.patch.object(extractor.client.events, 'upsert', return_value=None)
    mocker.patch.object(extractor, 'run_extraction_pipeline', return_value=None)
    mocker.patch.object(extractor, 'set_state', return_value=None)

    extractor.write_event_data_to_cdf(file_path, token, state_id)

    extractor.convert_lakehouse_data_to_df.assert_called_once_with(file_path, token)
    extractor.state_store.get_state.assert_called_once_with(state_id)
    extractor.client.events.upsert.assert_called_once_with([EventWrite()])
    extractor.run_extraction_pipeline.assert_called_once_with(status="success")
    extractor.set_state.assert_called_once_with(state_id, str(len(pd.DataFrame())))
