import pytest
from unittest.mock import patch, Mock
from cdf_fabric_replicator.event import EventsReplicator
from cognite.client.data_classes import Event
import pyarrow as pa

EVENT_BATCH_SIZE = 1

@pytest.fixture()
def test_event_replicator():
    event_replicator = EventsReplicator(metrics=Mock(), stop_event=Mock())
    event_replicator.config = Mock(event=Mock(batch_size=EVENT_BATCH_SIZE, lakehouse_abfss_path_events="Events"), extractor=Mock(poll_time=1))
    event_replicator.client = Mock()
    event_replicator.state_store = Mock()
    event_replicator.cognite_client = Mock()
    event_replicator.azure_credential = Mock(get_token=Mock(return_value=Mock(token="token")))
    return event_replicator

@pytest.fixture
def event():
    return {
        "external_id": "Test_Early_Notification_1710502959241",
        "data_set_id": 3351080165451477,
        "start_time": 1710502959241,
        "end_time": 1710503079241,
        "type": "Notification",
        "subtype": "HE3",
        "description": "Failed Instrumentation Fire extinguisher test",
        "metadata": {
            "description": "Failed Instrumentation Fire extinguisher test",
            "notification": "22081904",
            "work_order_number": "22082901",
            "notification_type": "HE3",
            "work_center": "SUPV",
        },
        "asset_ids": [5534771458240925],
        "id": 3631277804748961,
        "last_updated_time": 1710505316426,
        "created_time": 1710503304020,
    }

@patch("cdf_fabric_replicator.event.time.sleep")
@patch("cdf_fabric_replicator.event.EventsReplicator.process_events")
def test_run(mock_process_events, mock_sleep, test_event_replicator):
    test_event_replicator.stop_event = Mock(is_set=Mock(side_effect=[False, True]))
    test_event_replicator.run()
    test_event_replicator.state_store.initialize.assert_called_once()
    mock_process_events.assert_called_once()
    mock_sleep.assert_called_once()

@patch("cdf_fabric_replicator.event.logging.info")
def test_run_no_event_config(mock_logging, test_event_replicator):
    test_event_replicator.config.event = None
    test_event_replicator.run()
    mock_logging.assert_called_with("No event config found in config")

@pytest.mark.parametrize("last_created_time, event_query_time", [(None, 1), (1714685606, 1714685607)])
@patch("cdf_fabric_replicator.event.write_deltalake")
def test_process_events(mock_write_deltalake, event, test_event_replicator, last_created_time, event_query_time):
    # Set up empty state and cognite client events iterator
    test_event_replicator.state_store.get_state.return_value = [(None,last_created_time)]
    test_event_replicator.cognite_client.events = Mock(return_value=iter([Event(**event)]))

    # Run the process_events method
    test_event_replicator.process_events()

    # State store assertions
    test_event_replicator.state_store.get_state.assert_called_once_with(external_id="event_state")
    test_event_replicator.state_store.set_state.assert_called_once_with(external_id="event_state", high=event["created_time"])
    test_event_replicator.state_store.synchronize.assert_called_once()
    
    # Cognite client assertions
    test_event_replicator.cognite_client.events.assert_called_with(
        chunk_size=EVENT_BATCH_SIZE,
        created_time={"min": event_query_time},
        sort=("createdTime", "asc"),
    )
    pyarrow_event_data = pa.Table.from_pylist([Event(**event).dump()])
    mock_write_deltalake.assert_called_with(
        "Events",
        pyarrow_event_data,
        mode="append",
        engine="rust",
        schema_mode="merge",
        storage_options={
            "bearer_token": "token",
            "use_fabric_endpoint": "true",
        },
    )
