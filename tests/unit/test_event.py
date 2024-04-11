import pytest
from typing import Dict, List, Any
from cdf_fabric_replicator.event import EventsReplicator
from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.statestore import LocalStateStore
from cognite.client.data_classes import EventList, Event

EARLY_CREATED_TIME = 1710503304020
LATE_CREATED_TIME = 1710506651232

@pytest.fixture(scope="session")
def event_replicator(config):
    stop_event = CancellationToken()
    event_replicator = EventsReplicator(metrics=None, stop_event=stop_event)
    event_replicator.config = config
    event_replicator.client = event_replicator.config.cognite.get_cognite_client("test_event_replicator")
    event_replicator.state_store = LocalStateStore(
        event_replicator.config.extractor.state_store.local.path
    )

    return event_replicator


@pytest.fixture
def event_data():
    return [
        {
            'external_id': 'Test_Early_Notification_1710502959241', 
            'data_set_id': 3351080165451477, 
            'start_time': 1710502959241, 
            'end_time': 1710503079241, 
            'type': 'Notification', 
            'subtype': 'HE3', 
            'description': 'Failed Instrumentation Fire extinguisher test', 
            'metadata': {'description': 'Failed Instrumentation Fire extinguisher test', 'notification': '22081904', 'work_order_number': '22082901', 'notification_type': 'HE3', 'work_center': 'SUPV'}, 
            'asset_ids': [5534771458240925], 
            'id': 3631277804748961, 
            'last_updated_time': 1710505316426, 
            'created_time': EARLY_CREATED_TIME
        },
        {
            'external_id': 'Test_Late_Notification_1710506390376', 
            'data_set_id': 3351080165451477, 
            'start_time': 1710506390376, 
            'end_time': 1710506510376, 
            'type': 'Notification', 
            'subtype': 'HE3', 
            'description': 'Failed Instrumentation Fire extinguisher test', 
            'metadata': {'description': 'Failed Instrumentation Fire extinguisher test', 'notification': '22081904', 'notification_type': 'HE3', 'work_center': 'SUPV', 'work_order_number': '22082901'}, 
            'asset_ids': [5534771458240925], 
            'id': 5875589750425323, 
            'last_updated_time': 1710506651232, 
            'created_time': LATE_CREATED_TIME
        }
    ]

@pytest.fixture
def mock_get_events(mocker, event_data):
    event_collection = [Event(**data) for data in event_data]
    return_value = iter([EventList(event_collection)])
    return mocker.patch("cdf_fabric_replicator.event.EventsReplicator.get_events", return_value=return_value)


@pytest.fixture
def mock_get_late_events(mocker, event_data):
    return_value = iter([EventList([Event(**event_data[-1])])])
    return mocker.patch("cdf_fabric_replicator.event.EventsReplicator.get_events", return_value=return_value)

@pytest.fixture
def mock_get_no_events(mocker):
    return_value = iter([EventList([])])
    return mocker.patch("cdf_fabric_replicator.event.EventsReplicator.get_events", return_value=return_value)

@pytest.fixture
def mock_set_event_state(mocker):
    return mocker.patch("cdf_fabric_replicator.event.EventsReplicator.set_event_state", return_value=None)

@pytest.fixture
def mock_write_events_to_lakehouse_tables(mocker):
    return mocker.patch("cdf_fabric_replicator.event.EventsReplicator.write_events_to_lakehouse_tables", return_value=None)

def snake_to_camel(snake_str: str) -> str:
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def convert_dict_keys_to_camel_case(snake_case_dict: Dict[str, Any]) -> Dict[str, Any]:
    return {snake_to_camel(k): v for k, v in snake_case_dict.items()}

@pytest.fixture
def event_data_camel_case(event_data):
    return [convert_dict_keys_to_camel_case(data) for data in event_data]

def test_process_events_all_events(
        event_replicator: EventsReplicator, 
        event_data_camel_case: List[dict], 
        mock_get_events: Any, 
        mock_set_event_state: Any, 
        mock_write_events_to_lakehouse_tables: Any, 
        mocker):
    # Arrange
    mocker.patch("cdf_fabric_replicator.event.EventsReplicator.get_event_state", return_value=None)
    
    # Act
    event_replicator.process_events()

    # Assert
    mock_get_events.assert_called_with(event_replicator.config.event.batch_size, 0)
    mock_write_events_to_lakehouse_tables.assert_called_once()
    mock_write_events_to_lakehouse_tables.assert_called_with(
        event_data_camel_case, 
        event_replicator.config.event.lakehouse_abfss_path_events
    )
    mock_set_event_state.assert_called_with(
        event_replicator.event_state_key, 
        event_data_camel_case[-1]["createdTime"]
    )

def test_process_events_late_event(
        event_replicator: EventsReplicator, 
        event_data_camel_case: List[dict], 
        mock_get_late_events: Any, 
        mock_set_event_state: Any, 
        mock_write_events_to_lakehouse_tables: Any, 
        mocker):
    # Arrange
    mocker.patch("cdf_fabric_replicator.event.EventsReplicator.get_event_state", return_value=EARLY_CREATED_TIME)
    
    # Act
    event_replicator.process_events()
    
    # Assert
    mock_get_late_events.assert_called_with(event_replicator.config.event.batch_size, EARLY_CREATED_TIME)
    mock_write_events_to_lakehouse_tables.assert_called_once()
    mock_write_events_to_lakehouse_tables.assert_called_with(
        [event_data_camel_case[-1]], 
        event_replicator.config.event.lakehouse_abfss_path_events
    )
    mock_set_event_state.assert_called_with(
        event_replicator.event_state_key, 
        event_data_camel_case[-1]["createdTime"]
    )

def test_process_events_no_event(
        event_replicator: EventsReplicator, 
        mock_get_no_events: Any, 
        mock_set_event_state: Any, 
        mock_write_events_to_lakehouse_tables: Any, 
        mocker):
    # Arrange
    mocker.patch("cdf_fabric_replicator.event.EventsReplicator.get_event_state", return_value=LATE_CREATED_TIME)
    
    # Act
    event_replicator.process_events()
    
    # Assert
    mock_get_no_events.assert_called_with(event_replicator.config.event.batch_size, LATE_CREATED_TIME)
    mock_write_events_to_lakehouse_tables.assert_not_called()
    mock_set_event_state.assert_not_called()

