import pytest
import pandas as pd
from unittest.mock import patch, Mock
from cdf_fabric_replicator.extractor import CdfFabricExtractor
from cognite.client.data_classes import TimeSeriesWrite
from cognite.client.exceptions import CogniteNotFoundError, CogniteAPIError

TEST_DATA_SET_ID = 123456789101112
FILE_TIME = 1714798800


@pytest.fixture()
def test_extractor():
    with patch(
        "cdf_fabric_replicator.extractor.DefaultAzureCredential"
    ) as mock_credential:
        mock_credential.return_value.get_token.return_value = Mock(token="token")
        extractor = CdfFabricExtractor(stop_event=Mock(), metrics=Mock())
        extractor.config = Mock(
            source=Mock(raw_time_series_path="/table/path", read_batch_size=1000),
            destination=Mock(time_series_prefix="test_prefix"),
        )
        # These need to be mocked as they are not set in the constructor
        extractor.cognite_client = Mock()
        extractor.client = Mock()
        extractor.state_store = Mock()
        extractor.logger = Mock()
        extractor.data_set_id = TEST_DATA_SET_ID

        yield extractor


@pytest.fixture()
def mock_timeseries_data():
    return {
        "externalId": ["id1", "id1", "id1", "id1"],
        "timestamp": [
            "2022-02-07T16:01:27.001Z",
            "2022-02-07T16:03:47.000Z",
            "2022-02-07T16:03:52.000Z",
            "2022-02-07T16:04:12.000Z",
        ],
        "value": [1.0, 2.0, 3.0, 4.0],
    }


@pytest.fixture()
def mock_service_client():
    # File Object Mock
    mock_file = Mock()
    mock_file.is_directory = False
    mock_file.name = (
        "https://container@account.dfs.core.windows.net/Files/test_file.csv"
    )
    mock_file.last_modified.timestamp.return_value = FILE_TIME
    mock_file.creation_time.timestamp.return_value = FILE_TIME
    # File Client Mock
    mock_file_client = Mock()
    mock_file_client.download_file.return_value = Mock(
        readall=Mock(return_value=b"test_data")
    )
    # File System Client Mock
    mock_file_system_client = Mock()
    mock_file_system_client.get_file_client.return_value = mock_file_client
    mock_file_system_client.get_paths.return_value = [mock_file]
    # Service Client Mock
    mock_service_client = Mock()
    mock_service_client.get_file_system_client.return_value = mock_file_system_client
    yield mock_service_client


@pytest.fixture()
def event_data():
    return {
        "externalId": ["event1", "event2"],
        "startTime": [1633027200000, 1633113600000],  # Unix timestamp in milliseconds
        "endTime": [1633028200000, 1633114600000],  # Unix timestamp in milliseconds
        "type": ["type1", "type2"],
        "subtype": ["subtype1", "subtype2"],
        "metadata": [{"key1": "value1"}, {"key2": "value2"}],
        "description": ["description1", "description2"],
        "assetExternalIds": [["asset1", "asset2"], ["asset3", "asset4"]],
    }


def assert_state_store_calls(test_extractor, df, mock_timeseries_data, set_state=True):
    for external_id in list(set(mock_timeseries_data["externalId"])):
        test_extractor.state_store.get_state.assert_any_call(
            f"/table/path-{external_id}-state"
        )
        if set_state:
            test_extractor.state_store.set_state.assert_any_call(
                f"/table/path-{external_id}-state",
                df[df["externalId"] == external_id]["timestamp"].max(),
            )


@pytest.mark.parametrize(
    "raw_time_series_path, event_path, file_path, raw_tables, expected_calls",
    [
        (
            "table/timeseries_path",
            None,
            None,
            None,
            {"convert_lakehouse_data_to_df_batch": 1, "write_time_series_to_cdf": 1},
        ),
        (None, "table/event_path", None, None, {"write_event_data_to_cdf": 1}),
        (None, None, "files/file_path", None, {"upload_files_from_abfss": 1}),
    ],
)
@patch("cdf_fabric_replicator.extractor.CdfFabricExtractor.upload_files_from_abfss")
@patch("cdf_fabric_replicator.extractor.CdfFabricExtractor.write_event_data_to_cdf")
@patch("cdf_fabric_replicator.extractor.CdfFabricExtractor.write_time_series_to_cdf")
@patch(
    "cdf_fabric_replicator.extractor.CdfFabricExtractor.convert_lakehouse_data_to_df"
)
@patch(
    "cdf_fabric_replicator.extractor.CdfFabricExtractor.convert_lakehouse_data_to_df_batch",
    return_value=iter([pd.DataFrame()]),
)
@patch("cdf_fabric_replicator.extractor.CdfFabricExtractor.run_extraction_pipeline")
@patch("cdf_fabric_replicator.extractor.CdfFabricExtractor.get_current_statestore")
@patch("cdf_fabric_replicator.extractor.CdfFabricExtractor.get_current_config")
def test_extractor_run(
    mock_get_current_config,
    mock_get_current_statestore,
    mock_run_extraction_pipeline,
    mock_convert_lakehouse_data_to_df_batch,
    mock_convert_lakehouse_data_to_df,
    mock_write_time_series_to_cdf,
    mock_write_event_data_to_cdf,
    mock_upload_files_from_abfss,
    test_extractor,
    raw_time_series_path,
    event_path,
    file_path,
    raw_tables,
    expected_calls,
):
    # Set up a config with source paths in order to trigger the various extractor methods
    mock_get_current_config.return_value = Mock(
        source=Mock(
            data_set_id=str(TEST_DATA_SET_ID),
            abfss_prefix="abfss_prefix",
            raw_time_series_path=raw_time_series_path,
            event_path=event_path,
            file_path=file_path,
            raw_tables=raw_tables,
        ),
        destination=Mock(time_series_prefix="test_prefix"),
    )
    # Run the loop exactly once by setting the stop_event after the first run
    test_extractor.stop_event.is_set.side_effect = [False, True]

    # Call the method under test
    test_extractor.run()

    # Assert that the state store was initialized, the extraction pipeline was run, and the sleep method was called
    test_extractor.state_store.initialize.assert_called_once()
    mock_run_extraction_pipeline.assert_called_once_with(status="seen")

    # Assert extractor methods were called
    assert mock_convert_lakehouse_data_to_df.call_count == expected_calls.get(
        "convert_lakehouse_data_to_df", 0
    )
    assert mock_convert_lakehouse_data_to_df_batch.call_count == expected_calls.get(
        "convert_lakehouse_data_to_df_batch", 0
    )
    assert mock_write_time_series_to_cdf.call_count == expected_calls.get(
        "write_time_series_to_cdf", 0
    )
    assert mock_write_event_data_to_cdf.call_count == expected_calls.get(
        "write_event_data_to_cdf", 0
    )
    assert mock_upload_files_from_abfss.call_count == expected_calls.get(
        "upload_files_from_abfss", 0
    )


@patch("cdf_fabric_replicator.extractor.CdfFabricExtractor.get_current_statestore")
@patch(
    "cdf_fabric_replicator.extractor.CdfFabricExtractor.get_current_config",
    return_value=Mock(source=None),
)
def test_run_no_config_source(mock_config, mock_get_statestore, test_extractor):
    test_extractor.run()
    # Assert that an error was logged if no source path was provided
    test_extractor.logger.error.assert_called_once_with(
        "No source path or directory provided"
    )


@pytest.mark.parametrize(
    "last_update_time_index, expected_insert_dataframe_call_count, expected_set_state_call_count, expected_create_call_count",
    [
        (None, 1, 1, 0),  # test_write_time_series_to_cdf_all_new_data_points
        (1, 1, 1, 0),  # test_write_time_series_to_cdf_filter_old_data_points
        (-1, 0, 0, 0),  # test_write_time_series_to_cdf_no_new_data_points
    ],
)
def test_write_time_series_to_cdf(
    test_extractor,
    mock_timeseries_data,
    last_update_time_index,
    expected_insert_dataframe_call_count,
    expected_set_state_call_count,
    expected_create_call_count,
):
    # Create dataframe from test data
    df = pd.DataFrame(mock_timeseries_data)

    # Determine last update time based on the index
    last_update_time = (
        mock_timeseries_data["timestamp"][last_update_time_index]
        if last_update_time_index is not None
        else None
    )

    test_extractor.state_store.get_state.return_value = (last_update_time,)
    test_extractor.state_store.set_state.return_value = None
    test_extractor.client.time_series.data.insert_dataframe.return_value = None

    # Call the method under test
    test_extractor.write_time_series_to_cdf(df)

    # Assert state store calls
    assert_state_store_calls(
        test_extractor,
        df,
        mock_timeseries_data,
        set_state=expected_set_state_call_count > 0,
    )

    # Assert that the time series write was called the expected number of times
    assert (
        test_extractor.client.time_series.data.insert_dataframe.call_count
        == expected_insert_dataframe_call_count
    )

    # Assert that the time series create was called the expected number of times
    assert (
        test_extractor.client.time_series.create.call_count
        == expected_create_call_count
    )


def test_write_time_series_to_cdf_timeseries_not_found(
    test_extractor, mock_timeseries_data
):
    # Create dataframe from test data
    df = pd.DataFrame(mock_timeseries_data)

    test_extractor.state_store.get_state.return_value = (None,)
    test_extractor.state_store.set_state.return_value = None
    # Mock Cognite API to raise a CogniteNotFoundError
    test_extractor.client.time_series.data.insert_dataframe.side_effect = [
        CogniteNotFoundError(not_found=[{"externalId": "id1"}])
    ]

    test_extractor.write_time_series_to_cdf(df)

    assert_state_store_calls(test_extractor, df, mock_timeseries_data)

    # Assert time series write was called for the missing time series
    assert test_extractor.client.time_series.data.insert_dataframe.call_count == 1
    test_extractor.client.time_series.create.assert_called_once_with(
        TimeSeriesWrite(
            external_id="id1",
            is_string=True,
            name="id1",
            data_set_id=TEST_DATA_SET_ID,
        )
    )


def test_write_time_series_to_cdf_timeseries_retrieve_error(
    test_extractor, mock_timeseries_data
):
    # Create dataframe from test data
    df = pd.DataFrame(mock_timeseries_data)

    test_extractor.state_store.get_state.return_value = (None,)
    test_extractor.state_store.set_state.return_value = None
    # Mock Cognite API to raise a CogniteAPIError
    test_extractor.client.time_series.data.insert_dataframe.side_effect = [
        CogniteAPIError(code=500, message="Test error")
    ]
    # Call the method under test
    with pytest.raises(CogniteAPIError):
        test_extractor.write_time_series_to_cdf(df)
    # Assert error logger called
    test_extractor.logger.error.assert_called_once()


@pytest.mark.parametrize(
    "last_update_time, expected_upsert_call_count, expected_run_extraction_pipeline_call_count",
    [
        (1, 1, 1),  # test_write_event_data_to_cdf_new_events
        (2, 0, 0),  # test_write_event_data_to_cdf_no_new_events
    ],
)
@patch("cdf_fabric_replicator.extractor.CdfFabricExtractor.run_extraction_pipeline")
def test_write_event_data_to_cdf(
    mock_run_extraction_pipeline,
    event_data,
    test_extractor,
    mocker,
    last_update_time,
    expected_upsert_call_count,
    expected_run_extraction_pipeline_call_count,
):
    mocker.patch(
        "cdf_fabric_replicator.extractor.DeltaTable",
        return_value=Mock(to_pandas=Mock(return_value=pd.DataFrame(data=event_data))),
    )
    test_extractor.state_store.get_state.return_value = (last_update_time,)

    # Call the method under test
    test_extractor.write_event_data_to_cdf("file_path", "token", "state_id")

    # Assert that the upsert method was called the expected number of times
    assert test_extractor.client.events.upsert.call_count == expected_upsert_call_count

    # Assert that the run_extraction_pipeline method was called the expected number of times
    assert (
        mock_run_extraction_pipeline.call_count
        == expected_run_extraction_pipeline_call_count
    )


def test_write_event_data_asset_ids_not_found(test_extractor, event_data, mocker):
    mocker.patch(
        "cdf_fabric_replicator.extractor.DeltaTable",
        return_value=Mock(to_pandas=Mock(return_value=pd.DataFrame(data=event_data))),
    )
    test_extractor.state_store.get_state.return_value = (None,)
    # Mock Cognite API to raise a CogniteNotFoundError
    test_extractor.client.assets.retrieve.side_effect = [
        CogniteNotFoundError(not_found=[{"externalId": "asset1"}])
    ]
    # Call the method under test
    with pytest.raises(CogniteNotFoundError):
        test_extractor.write_event_data_to_cdf("file_path", "token", "state_id")
    # Assert error logger called
    test_extractor.logger.error.assert_called_once()


def test_write_event_data_asset_retrieve_error(test_extractor, event_data, mocker):
    mocker.patch(
        "cdf_fabric_replicator.extractor.DeltaTable",
        return_value=Mock(to_pandas=Mock(return_value=pd.DataFrame(data=event_data))),
    )
    test_extractor.state_store.get_state.return_value = (None,)
    # Mock Cognite API to raise a CogniteNotFoundError
    test_extractor.client.assets.retrieve.side_effect = [
        CogniteAPIError(code=500, message="Test error")
    ]
    # Call the method under test
    with pytest.raises(CogniteAPIError):
        test_extractor.write_event_data_to_cdf("file_path", "token", "state_id")
    # Assert error logger called
    test_extractor.logger.error.assert_called_once()


def test_write_event_data_to_cdf_upsert_error(test_extractor, event_data, mocker):
    mocker.patch(
        "cdf_fabric_replicator.extractor.DeltaTable",
        return_value=Mock(to_pandas=Mock(return_value=pd.DataFrame(data=event_data))),
    )
    test_extractor.state_store.get_state.return_value = (None,)
    # Mock Cognite API to raise a CogniteAPIError
    test_extractor.client.events.upsert.side_effect = [
        CogniteAPIError(code=500, message="Test error")
    ]
    # Call the method under test
    with pytest.raises(CogniteAPIError):
        test_extractor.write_event_data_to_cdf("file_path", "token", "state_id")
    # Assert error logger called
    test_extractor.logger.error.assert_called_once()


def test_upload_files_from_abfss(mock_service_client, test_extractor, mocker):
    url = "https://container@account.dfs.core.windows.net/Files"
    mocker.patch(
        "cdf_fabric_replicator.extractor.DataLakeServiceClient",
        return_value=mock_service_client,
    )
    test_extractor.state_store.get_state.return_value = (None,)
    # Call the method under test
    test_extractor.upload_files_from_abfss(url)

    # Assert that upload_bytes was called with the correct arguments
    test_extractor.cognite_client.files.upload_bytes.assert_called_once_with(
        content=b"test_data",
        name="test_file.csv",
        external_id="https://container@account.dfs.core.windows.net/Files/test_file.csv",
        data_set_id=TEST_DATA_SET_ID,
        source_created_time=int(FILE_TIME * 1000),
        source_modified_time=int(FILE_TIME * 1000),
        overwrite=True,
    )


def test_upload_files_from_abfss_cognite_error(
    mock_service_client, test_extractor, mocker
):
    url = "https://container@account.dfs.core.windows.net/Files"
    mocker.patch(
        "cdf_fabric_replicator.extractor.DataLakeServiceClient",
        return_value=mock_service_client,
    )
    test_extractor.state_store.get_state.return_value = (None,)

    # Mock Cognite API to raise a CogniteAPIError
    test_extractor.cognite_client.files.upload_bytes.side_effect = [
        CogniteAPIError(code=500, message="Test error")
    ]
    # Call the method under test
    with pytest.raises(CogniteAPIError):
        test_extractor.upload_files_from_abfss(url)

    test_extractor.logger.error.assert_called_once()


def test_convert_lakehouse_data_to_df_exception(test_extractor, mocker):
    # Mock DeltaTable to raise exception from to_pandas
    mocker.patch(
        "cdf_fabric_replicator.extractor.DeltaTable",
        return_value=Mock(to_pandas=Mock(side_effect=Exception("Test error"))),
    )
    # Assert Exception was raised by function
    with pytest.raises(Exception):
        test_extractor.convert_lakehouse_data_to_df("path", "token")
    # Assert error logger called
    test_extractor.logger.error.assert_called_once()


def test_convert_lakehouse_data_to_df_batch_exception(test_extractor, mocker):
    # Mock DeltaTable to raise exception from to_pandas
    mocker.patch(
        "cdf_fabric_replicator.extractor.DeltaTable",
        return_value=Mock(
            to_pyarrow_dataset=Mock(
                return_value=Mock(
                    to_batches=Mock(
                        return_value=iter(
                            [Mock(to_pandas=Mock(side_effect=Exception("Test error")))]
                        )
                    )
                )
            )
        ),
    )
    # Assert Exception was raised by function
    with pytest.raises(Exception):
        for x in test_extractor.convert_lakehouse_data_to_df_batch("path", "token"):
            pass
    # Assert error logger called
    test_extractor.logger.error.assert_called_once()


def test_run_extraction_pipeline_cognite_error(test_extractor, mocker):
    # Mock Cognite API to raise a CogniteAPIError
    test_extractor.cognite_client.extraction_pipelines.runs.create.side_effect = [
        CogniteAPIError(code=500, message="Test error")
    ]
    # Assert Exception was raised by function
    with pytest.raises(CogniteAPIError):
        test_extractor.run_extraction_pipeline(status="seen")
    # Assert error logger called
    test_extractor.logger.error.assert_called_once()


def test_write_raw_tables_cdf(test_extractor, mocker):
    FILE_PATH = "test_file_path"
    TOKEN = "test_token"
    STATE_ID = "/table/path-state"
    DB_NAME = "test_db"
    TABLE_NAME = "test_table"
    DATAFRAME = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})

    mocker.patch.object(
        test_extractor, "convert_lakehouse_data_to_df", return_value=DATAFRAME
    )
    mocker.patch.object(test_extractor.state_store, "get_state", return_value=(2,))
    mocker.patch.object(
        test_extractor.client.raw.rows, "insert_dataframe", return_value=None
    )
    mocker.patch.object(test_extractor, "run_extraction_pipeline", return_value=None)
    mocker.patch.object(test_extractor, "set_state", return_value=None)

    test_extractor.write_raw_tables_to_cdf(
        FILE_PATH, TOKEN, STATE_ID, TABLE_NAME, DB_NAME
    )

    test_extractor.convert_lakehouse_data_to_df.assert_called_once_with(
        FILE_PATH, TOKEN
    )
    test_extractor.state_store.get_state.assert_called_once_with(STATE_ID)
    test_extractor.client.raw.rows.insert_dataframe.assert_called_once_with(
        db_name=DB_NAME, table_name=TABLE_NAME, dataframe=DATAFRAME, ensure_parent=True
    )
    test_extractor.run_extraction_pipeline.assert_called_once_with(status="success")
    test_extractor.set_state.assert_called_once_with(STATE_ID, str(len(DATAFRAME)))


def test_write_raw_tables_no_change_cdf(test_extractor, mocker):
    FILE_PATH = "test_file_path"
    TOKEN = "test_token"
    STATE_ID = "/table/path-state"
    DB_NAME = "test_db"
    TABLE_NAME = "test_table"
    DATAFRAME = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})

    mocker.patch.object(
        test_extractor, "convert_lakehouse_data_to_df", return_value=DATAFRAME
    )
    mocker.patch.object(test_extractor.state_store, "get_state", return_value=(3,))
    mocker.patch.object(test_extractor, "run_extraction_pipeline", return_value=None)
    mocker.patch.object(test_extractor, "set_state", return_value=None)

    test_extractor.write_raw_tables_to_cdf(
        FILE_PATH, TOKEN, STATE_ID, TABLE_NAME, DB_NAME
    )

    test_extractor.convert_lakehouse_data_to_df.assert_called_once_with(
        FILE_PATH, TOKEN
    )
    test_extractor.state_store.get_state.assert_called_once_with(STATE_ID)
    test_extractor.run_extraction_pipeline.assert_called_once_with(status="seen")


def test_write_raw_tables_error_on_insert(test_extractor, mocker):
    FILE_PATH = "test_file_path"
    TOKEN = "test_token"
    STATE_ID = "/table/path-state"
    DB_NAME = "test_db"
    TABLE_NAME = "test_table"
    DATAFRAME = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})

    mocker.patch.object(
        test_extractor, "convert_lakehouse_data_to_df", return_value=DATAFRAME
    )
    mocker.patch.object(test_extractor.state_store, "get_state", return_value=(2,))
    mocker.patch.object(test_extractor, "run_extraction_pipeline", return_value=None)
    mocker.patch.object(
        test_extractor.client.raw.rows, "insert_dataframe", return_value=None
    )
    # Mock Cognite API to raise a CogniteAPIError
    test_extractor.client.raw.rows.insert_dataframe.side_effect = [
        CogniteAPIError(code=503, message="Test error")
    ]
    # Call the method under test
    with pytest.raises(CogniteAPIError):
        test_extractor.write_raw_tables_to_cdf(
            FILE_PATH, TOKEN, STATE_ID, TABLE_NAME, DB_NAME
        )

    test_extractor.convert_lakehouse_data_to_df.assert_called_once_with(
        FILE_PATH, TOKEN
    )

    test_extractor.client.raw.rows.insert_dataframe.assert_called_once_with(
        db_name=DB_NAME, table_name=TABLE_NAME, dataframe=DATAFRAME, ensure_parent=True
    )
    test_extractor.run_extraction_pipeline.assert_called_once_with(status="failure")
