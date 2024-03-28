import os
import pytest
from unittest.mock import Mock
from azure.identity import DefaultAzureCredential
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.metrics import safe_get
from cdf_fabric_replicator.metrics import Metrics
from cdf_fabric_replicator.time_series import TimeSeriesReplicator
from cdf_fabric_replicator.data_modeling import DataModelingReplicator
from dotenv import load_dotenv
from tests.integration.integration_steps.cdf_steps import remove_time_series_data, push_time_series_to_cdf, create_subscription_in_cdf
from tests.integration.integration_steps.fabric_steps import get_ts_delta_table
from tests.integration.integration_steps.time_series_generation import generate_timeseries_set

load_dotenv()

@pytest.fixture(scope="function")
def test_replicator():
    stop_event = CancellationToken()
    replicator = TimeSeriesReplicator(metrics=safe_get(Metrics), stop_event=stop_event)
    replicator._initial_load_config(override_path=os.environ["TEST_CONFIG_PATH"])
    replicator.cognite_client = replicator.config.cognite.get_cognite_client(replicator.name)
    replicator._load_state_store()
    replicator.logger = Mock()
    yield replicator
    try:
        os.remove("states.json")
    except FileNotFoundError:
        pass


@pytest.fixture(scope="function")
def test_data_modeling_replicator():
    stop_event = CancellationToken()
    replicator = DataModelingReplicator(metrics=safe_get(Metrics), stop_event=stop_event)
    replicator._initial_load_config(override_path=os.environ["TEST_CONFIG_PATH"])
    replicator.cognite_client = replicator.config.cognite.get_cognite_client(replicator.name)
    replicator._load_state_store()
    replicator.logger = Mock()
    yield replicator
    try:
        os.remove("states.json")
    except FileNotFoundError:
        pass


@pytest.fixture(scope="session")
def cognite_client():
    credentials = OAuthClientCredentials(
        token_url=os.environ["COGNITE_TOKEN_URL"],
        client_id=os.environ["COGNITE_CLIENT_ID"],
        client_secret=os.environ["COGNITE_CLIENT_SECRET"],
        scopes=os.environ["COGNITE_TOKEN_SCOPES"].split(","),
    )

    return CogniteClient(
        ClientConfig(
            client_name=os.environ["COGNITE_CLIENT_NAME"],
            project=os.environ["COGNITE_PROJECT"],
            base_url=os.environ["COGNITE_BASE_URL"],
            credentials=credentials,
        )
    )

@pytest.fixture(scope="session")
def azure_credential():
    return DefaultAzureCredential()

@pytest.fixture(scope="session")
def lakehouse_timeseries_path(azure_credential):
    lakehouse_timeseries_path = os.environ["LAKEHOUSE_ABFSS_PREFIX"] + "/Tables/" + os.environ["DPS_TABLE_NAME"]
    yield lakehouse_timeseries_path
    delta_table = get_ts_delta_table(azure_credential, lakehouse_timeseries_path)
    delta_table.delete()

@pytest.fixture()
def time_series(request, cognite_client):
    sub_name = "testSubscription"
    timeseries_set = generate_timeseries_set(request.param)
    remove_time_series_data(timeseries_set, sub_name, cognite_client)
    push_time_series_to_cdf(timeseries_set, cognite_client)
    create_subscription_in_cdf(timeseries_set, sub_name, cognite_client)
    yield timeseries_set
    remove_time_series_data(timeseries_set, sub_name, cognite_client)