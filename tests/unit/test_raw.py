import pytest
from unittest.mock import patch, Mock
from cdf_fabric_replicator.raw import RawTableReplicator
from cognite.client.data_classes import Row
from deltalake.exceptions import DeltaError


@pytest.fixture()
def test_raw_replicator():
    with patch("cdf_fabric_replicator.raw.DefaultAzureCredential") as mock_credential:
        mock_credential.return_value.get_token.return_value = Mock(token="token")
        raw_replicator = RawTableReplicator(metrics=Mock(), stop_event=Mock())
        raw_replicator.config = Mock(
            raw_tables=[
                Mock(
                    table_name="raw_table",
                    db_name="raw_db",
                    lakehouse_abfss_path_raw="abfss://raw",
                )
            ],
            extractor=Mock(poll_time=1),
        )
        raw_replicator.client = Mock()
        raw_replicator.state_store = Mock()
        raw_replicator.cognite_client = Mock()
        raw_replicator.logger = Mock()
        return raw_replicator


@pytest.fixture
def rowList():
    return [
        Row(
            key="Test_Early_Notification_1710502959241",
            columns={
                "type": "Notification",
                "subtype": "HE3",
                "description": "Failed Instrumentation Fire extinguisher test",
            },
            last_updated_time=1710505316426,
        )
    ]


@patch("cdf_fabric_replicator.raw.RawTableReplicator.process_raw_tables")
def test_run(mock_process_raw_tables, test_raw_replicator):
    test_raw_replicator.stop_event = Mock(is_set=Mock(side_effect=[False, True]))
    test_raw_replicator.run()
    test_raw_replicator.state_store.initialize.assert_called_once()
    mock_process_raw_tables.assert_called_once()


def test_run_no_event_config(test_raw_replicator):
    test_raw_replicator.config.raw_tables = None
    test_raw_replicator.run()
    test_raw_replicator.logger.warning.assert_called_with(
        "No Raw config found in config"
    )


@pytest.mark.parametrize("last_updated_time", [None, 1714685607])
@patch("cdf_fabric_replicator.raw.write_deltalake")
def test_process_raw_tables(
    mock_write_deltalake,
    rowList,
    test_raw_replicator,
    last_updated_time,
):
    # Set up empty state and cognite client events iterator
    test_raw_replicator.state_store.get_state.return_value = [(None, last_updated_time)]
    test_raw_replicator.cognite_client.raw.rows.list = Mock(return_value=rowList)

    # Run the process_events method
    test_raw_replicator.process_raw_tables()

    # State store assertions
    test_raw_replicator.state_store.get_state.assert_called_once_with(
        external_id="raw_db_raw_table_raw_state"
    )
    test_raw_replicator.state_store.set_state.assert_called_once_with(
        external_id="raw_db_raw_table_raw_state", high=rowList["last_updated_time"]
    )
    test_raw_replicator.state_store.synchronize.assert_called_once()

    # Cognite client assertions
    test_raw_replicator.cognite_client.raw.rows.list.assert_called_with(
        db_name="raw_db",
        table_name="raw_table",
        min_last_updated_time=last_updated_time,
        limit=test_raw_replicator.config.extractor.fabric_ingest_batch_size,
    )

    mock_write_deltalake.assert_called_once_with()


@patch("cdf_fabric_replicator.raw.write_deltalake")
def test_process_events_delta_error(mock_write_deltalake, rowList, test_raw_replicator):
    # Set up empty state and cognite client events iterator
    test_raw_replicator.state_store.get_state.return_value = [(None, None)]
    test_raw_replicator.cognite_client.events = Mock(return_value=rowList)
    # Set up mock write_deltalake to raise DeltaError
    mock_write_deltalake.side_effect = DeltaError()

    # Run the process_events method
    with pytest.raises(DeltaError):
        test_raw_replicator.process_raw_tables()
    # Assert logger call
    test_raw_replicator.logger.error.call_count == 2
