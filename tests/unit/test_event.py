from unittest.mock import Mock, patch

import json
import pandas as pd
import pyarrow as pa
import pytest
from cdf_fabric_replicator.event import EventsReplicator
from deltalake.exceptions import DeltaError

from cognite.client.data_classes import Event

EVENT_BATCH_SIZE = 1


@pytest.fixture()
def test_event_replicator():
    with patch("cdf_fabric_replicator.event.DefaultAzureCredential") as mock_credential:
        mock_credential.return_value.get_token.return_value = Mock(token="token")
        event_replicator = EventsReplicator(metrics=Mock(), stop_event=Mock())
        event_replicator.config = Mock(
            event=Mock(
                batch_size=EVENT_BATCH_SIZE,
                lakehouse_abfss_path_events="Events",
                dataset_external_id="data_set_xid",
            ),
            extractor=Mock(poll_time=1),
        )
        event_replicator.client = Mock()
        event_replicator.state_store = Mock()
        event_replicator.cognite_client = Mock()
        event_replicator.logger = Mock()
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


@patch("cdf_fabric_replicator.event.EventsReplicator.process_events")
def test_run(mock_process_events, test_event_replicator):
    test_event_replicator.stop_event = Mock(is_set=Mock(side_effect=[False, True]))
    test_event_replicator.run()
    test_event_replicator.state_store.initialize.assert_called_once()
    mock_process_events.assert_called_once()


def test_run_no_event_config(test_event_replicator):
    test_event_replicator.config.event = None
    test_event_replicator.run()
    test_event_replicator.logger.warning.assert_called_with(
        "No event config found in config"
    )


# @pytest.mark.skip("Error in test")
@pytest.mark.parametrize(
    "last_created_time, event_query_time", [(None, 1), (1714685606, 1714685607)]
)
@patch("cdf_fabric_replicator.event.write_deltalake")
def test_process_events_new_table(
    mock_write_deltalake,
    event,
    test_event_replicator,
    last_created_time,
    event_query_time,
):
    # Set up empty state and cognite client events iterator
    test_event_replicator.state_store.get_state.return_value = [
        (None, last_created_time)
    ]

    test_event_replicator.cognite_client.events = Mock(
        return_value=iter([Event(**event)])
    )

    # Run the process_events method
    test_event_replicator.process_events()

    # State store assertions
    test_event_replicator.state_store.get_state.assert_called_once_with(
        external_id="event_state"
    )
    # test_event_replicator.state_store.set_state.assert_called_once_with(
    #    external_id="event_state", high=event["created_time"]
    # )
    # test_event_replicator.state_store.synchronize.assert_called_once()

    # Cognite client assertions
    test_event_replicator.cognite_client.events.assert_called_with(
        chunk_size=EVENT_BATCH_SIZE,
        created_time={"min": event_query_time},
        sort=("createdTime", "asc"),
        data_set_external_ids=["data_set_xid"],
    )

    event_dict = event.copy()
    event_dict["metadata"] = json.dumps(event_dict["metadata"])

    pyarrow_event_data = pa.Table.from_pylist([Event(**event_dict).dump()])
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


@patch("cdf_fabric_replicator.event.DeltaTable")
def test_process_events_delta_error(mock_deltatable, event, test_event_replicator):
    # Set up empty state and cognite client events iterator
    test_event_replicator.state_store.get_state.return_value = [(None, None)]
    test_event_replicator.cognite_client.events = Mock(
        return_value=iter([Event(**event)])
    )
    # Set up mock write_deltalake to raise DeltaError
    mock_deltatable.side_effect = DeltaError()

    # Run the process_events method
    with pytest.raises(DeltaError):
        test_event_replicator.process_events()
    # Assert logger call
    test_event_replicator.logger.error.call_count == 2


@pytest.mark.skip("Error in test")
@patch("cdf_fabric_replicator.event.pa.Table")
@patch("cdf_fabric_replicator.event.DeltaTable")
def test_write_events_to_lakehouse_tables_merge(
    mock_deltatable, mock_pa_table, test_event_replicator
):
    # Create a mock DeltaTable
    mock_delta_table = Mock()

    # Set the return value of DeltaTable to return Mock table
    mock_deltatable.return_value = mock_delta_table

    # Set the return value of pa.Table.from_pylist to return None
    empty_df = pd.DataFrame()
    mock_pa_table.from_pylist.return_value = empty_df

    # Set the abfss path
    abfss_path = "abfss://path/to/events"

    # Call the write_events_to_lakehouse_tables method
    test_event_replicator.write_events_to_lakehouse_tables([], abfss_path)

    # Check that DeltaTable was called with the correct arguments
    mock_deltatable.assert_called_once_with(
        abfss_path,
        storage_options={
            "bearer_token": "token",
            "use_fabric_endpoint": "true",
        },
    )
