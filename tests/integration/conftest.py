import pytest
import os
from dotenv import load_dotenv
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from azure.identity import DefaultAzureCredential
from tests.integration.integration_steps.cdf_steps import remove_time_series_data
from tests.integration.integration_steps.time_series_generation import generate_timeseries_set
from tests.integration.integration_steps.fabric_steps import get_ts_delta_table

load_dotenv()

TEST_TIMESERIES_TABLE_NAME = "timeseries"

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
    lakehouse_timeseries_path = os.environ["LAKEHOUSE_ABFSS_PREFIX"] + "/Tables/" + TEST_TIMESERIES_TABLE_NAME
    yield lakehouse_timeseries_path
    delta_table = get_ts_delta_table(azure_credential, lakehouse_timeseries_path)
    delta_table.delete()

@pytest.fixture()
def time_series(request, cognite_client):
    timeseries_set = generate_timeseries_set(request.param)
    yield timeseries_set
    remove_time_series_data(timeseries_set, cognite_client)
