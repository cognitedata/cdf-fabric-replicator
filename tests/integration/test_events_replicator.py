import os
import pandas as pd
from typing import List
from datetime import datetime
from time import sleep
import pytest
from unittest.mock import Mock
from cognite.client import CogniteClient
from azure.identity import DefaultAzureCredential
from cognite.client.data_classes import EventWrite
from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.metrics import safe_get
from integration_steps.cdf_steps import (
    push_events_to_cdf,
    remove_events_from_cdf,
    delete_event_state_store_in_cdf,
)
from integration_steps.fabric_steps import (
    assert_events_data_in_fabric,
    delete_delta_table_data,
)
from integration_steps.service_steps import run_events_replicator
from cdf_fabric_replicator.metrics import Metrics
from cdf_fabric_replicator.event import EventsReplicator

EVENT_DURATION = 60000  # 1 minute


@pytest.fixture(scope="function")
def test_event_replicator(request):
    stop_event = CancellationToken()
    replicator = EventsReplicator(metrics=safe_get(Metrics), stop_event=stop_event)
    replicator._initial_load_config(override_path=os.environ["TEST_CONFIG_PATH"])
    replicator.cognite_client = replicator.config.cognite.get_cognite_client(
        replicator.name
    )
    replicator._load_state_store()
    replicator.logger = Mock()
    replicator.config.event.batch_size = (
        request.param
    )  # Set batch size using the indirect parameter
    yield replicator
    delete_event_state_store_in_cdf(
        replicator.config.extractor.state_store.raw.database,
        replicator.config.extractor.state_store.raw.table,
        replicator.event_state_key,
        replicator.cognite_client,
    )


@pytest.fixture()
def event_write_list(request, cognite_client):
    # Remove existing events from test environment
    environment_events = cognite_client.events.list(limit=None)
    remove_events_from_cdf(cognite_client, environment_events.data)

    # Create new events
    current_timestamp = int(
        datetime.now().timestamp() * 1000
    )  # Current timestamp in milliseconds
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
    df = pd.DataFrame(events_dict, index=[i for i in range(len(events_dict))])
    return df


@pytest.fixture()
def events_path(azure_credential: DefaultAzureCredential):
    path = (
        os.environ["LAKEHOUSE_ABFSS_PREFIX"]
        + "/Tables/"
        + os.environ["EVENT_TABLE_NAME"]
    )
    delete_delta_table_data(azure_credential, path)
    yield path
    delete_delta_table_data(azure_credential, path)


# The parameterized fixtures will be executed for each parameter combination
@pytest.mark.parametrize(
    "event_write_list",
    [10, 25, 100],
    indirect=True,
)  # Number of events to be created in CDF
@pytest.mark.parametrize(
    "test_event_replicator",
    [10, 100],
    indirect=True,
)  # Batch size for the replicator
def test_events_service(
    cognite_client,
    azure_credential,
    test_event_replicator,
    event_write_list,
    events_dataframe,
    events_path,
):
    # Given events populated in CDF
    push_events_to_cdf(cognite_client, event_write_list)
    sleep(5)  # 5 second sleep to allow events to be populated in CDF

    # When the events replicator runs
    run_events_replicator(test_event_replicator)

    # Then events will be available in fabric
    assert_events_data_in_fabric(events_path, events_dataframe, azure_credential)
