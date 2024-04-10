import pytest
from cdf_fabric_replicator.event import EventsReplicator
from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.statestore import LocalStateStore
from cognite.client.data_classes import EventList, Event


@pytest.fixture(scope="session")
def event_replicator(config):
    stop_event = CancellationToken()
    event_replicator = EventsReplicator(metrics=None, stop_event=stop_event)
    event_replicator.config = config
    event_replicator.client = event_replicator.config.cognite.get_cognite_client("test_event_replicator")
    event_replicator.state_store = LocalStateStore(
        event_replicator.config.extractor.state_store.local.path
    )

    yield event_replicator

@pytest.fixture
def early_created_time():
    return 1710503304020

@pytest.fixture
def late_created_time():
    return 1710506651232

@pytest.fixture
def event_data(early_created_time, late_created_time):
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
            'created_time': early_created_time
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
            'created_time': late_created_time
        }
    ]

@pytest.fixture
def mock_get_events(mocker, event_data):
    return_value = iter([EventList([Event(**data) for data in event_data])])
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

def snake_to_camel(snake_str):
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def convert_dict_keys_to_camel_case(snake_case_dict):
    return {snake_to_camel(k): v for k, v in snake_case_dict.items()}

@pytest.fixture
def event_data_camel_case(event_data):
    return [convert_dict_keys_to_camel_case(data) for data in event_data]

def test_write_all_events_to_fabric(event_replicator, event_data_camel_case, mock_get_events, mock_set_event_state, mock_write_events_to_lakehouse_tables, mocker):
    mocker.patch("cdf_fabric_replicator.event.EventsReplicator.get_event_state", return_value=None)
    event_replicator.process_events()
    mock_get_events.assert_called_with(event_replicator.config.event.batch_size, 0)
    mock_write_events_to_lakehouse_tables.assert_called_once()
    mock_write_events_to_lakehouse_tables.assert_called_with(
        event_data_camel_case, 
        event_replicator.config.event.lakehouse_abfss_prefix
    )
    mock_set_event_state.assert_called_with(
        event_replicator.event_state_key, 
        event_data_camel_case[-1]["createdTime"]
    )

def test_write_late_events_to_fabric(event_replicator, early_created_time, event_data_camel_case, mock_get_late_events, mock_set_event_state, mock_write_events_to_lakehouse_tables, mocker):
    mocker.patch("cdf_fabric_replicator.event.EventsReplicator.get_event_state", return_value=early_created_time)
    event_replicator.process_events()
    mock_get_late_events.assert_called_with(event_replicator.config.event.batch_size, early_created_time)
    mock_write_events_to_lakehouse_tables.assert_called_once()
    mock_write_events_to_lakehouse_tables.assert_called_with(
        [event_data_camel_case[-1]], 
        event_replicator.config.event.lakehouse_abfss_prefix
    )
    mock_set_event_state.assert_called_with(
        event_replicator.event_state_key, 
        event_data_camel_case[-1]["createdTime"]
    )

def test_write_no_events_to_fabric(event_replicator, late_created_time, event_data, mock_get_no_events, mock_set_event_state, mock_write_events_to_lakehouse_tables, mocker):
    mocker.patch("cdf_fabric_replicator.event.EventsReplicator.get_event_state", return_value=late_created_time)
    event_replicator.process_events()
    mock_get_no_events.assert_called_with(event_replicator.config.event.batch_size, late_created_time)
    mock_write_events_to_lakehouse_tables.assert_not_called()
    mock_set_event_state.assert_not_called()
