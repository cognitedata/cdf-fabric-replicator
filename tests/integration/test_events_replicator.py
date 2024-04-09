import os
import pandas as pd
from typing import List
from datetime import datetime
import pytest
from azure.identity import DefaultAzureCredential
from integration_steps.cdf_steps import push_events_to_cdf, remove_events_from_cdf
from integration_steps.fabric_steps import assert_events_data_in_fabric, delete_delta_table_data
from integration_steps.service_steps import run_events_replicator
from cognite.client.data_classes import EventWrite

EVENT_DURATION = 60000 # 1 minute

@pytest.fixture()
def event_write_list(request, cognite_client):
    # Remove existing events from test environment
    environment_events = cognite_client.events.list(limit=None)
    remove_events_from_cdf(cognite_client, environment_events.data)

    # Create new events
    current_timestamp = int(datetime.now().timestamp() * 1000) # Current timestamp in milliseconds
    events = [
        EventWrite(
            external_id=f"Notification_{current_timestamp + i}",
            description=f"Event {i}",
            start_time=current_timestamp + i,
            end_time=current_timestamp + i + EVENT_DURATION,
            type="Notification",
            subtype="Test",
        )
        for i in range(request.param)
    ]

    yield events

    # Clean up after the test
    remove_events_from_cdf(cognite_client, events)

@pytest.fixture()
def events_dataframe(event_write_list: List[EventWrite]) -> pd.DataFrame:
    events_dict = [event.dump() for event in event_write_list]
    df = pd.DataFrame(events_dict)
    return df

@pytest.fixture()
def events_path(azure_credential: DefaultAzureCredential):
    path = os.environ["LAKEHOUSE_ABFSS_PREFIX"] + "/Tables/Events"
    delete_delta_table_data(azure_credential, path)
    yield path
    delete_delta_table_data(azure_credential, path)

# The parameterized fixtures will be executed for each parameter combination
@pytest.mark.parametrize("event_write_list", [10, 100], indirect=True,) # Number of events to be created in CDF
@pytest.mark.parametrize("batch_size", [10, 100]) # Batch size for the replicator
def test_events_service(cognite_client, azure_credential, batch_size, event_write_list, events_dataframe, events_path):
    # Given events populated in CDF
    push_events_to_cdf(cognite_client, event_write_list)

    # When the events replicator runs
    run_events_replicator(batch_size)

    # Then events will be available in fabric
    assert_events_data_in_fabric(events_path, events_dataframe, azure_credential)