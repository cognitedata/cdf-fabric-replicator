import os
import pytest
import yaml
from typing import cast
from unittest.mock import Mock
from azure.identity import DefaultAzureCredential
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from pathlib import Path
from cognite.client.data_classes.data_modeling import (
    Space, 
    SpaceApply,
    DataModel,
    View,
    NodeApply,
    EdgeApply,
    SingleHopConnectionDefinition
)
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.metrics import safe_get
from cognite.extractorutils.base import CancellationToken
from deltalake.exceptions import TableNotFoundError
from cdf_fabric_replicator.metrics import Metrics
from cdf_fabric_replicator.time_series import TimeSeriesReplicator
from dotenv import load_dotenv
from tests.integration.integration_steps.cdf_steps import remove_time_series_data, push_time_series_to_cdf, create_subscription_in_cdf
from tests.integration.integration_steps.fabric_steps import get_ts_delta_table
from tests.integration.integration_steps.time_series_generation import generate_timeseries_set
from integration_steps.data_model_generation import create_actor, create_movie, create_edge

load_dotenv()
RESOURCES = Path(__file__).parent / "resources"

def lakehouse_table_name(table_name:str):
    return os.environ["LAKEHOUSE_ABFSS_PREFIX"] + "/Tables/" + table_name

@pytest.fixture(scope="session")
def test_config():
    with open(os.environ["TEST_CONFIG_PATH"]) as file:
        config = yaml.safe_load(file)
    return config

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

@pytest.fixture(scope="session")
def test_space(test_config, cognite_client: CogniteClient):
    space_id = test_config["data_modeling"][0]["space"]
    space = cognite_client.data_modeling.spaces.retrieve(space_id)
    if space is None:
        new_space = SpaceApply(space_id, name="Integration Test Space", description="The space used for integration tests.")
        space = cognite_client.data_modeling.spaces.apply(new_space)
    else: # Ensure there aren't existing data models
        data_model_list = cognite_client.data_modeling.data_models.list(space=space_id)
        assert len(data_model_list) == 0, "Space should not have existing data models, please remove models/views/containers before testing"
    yield space
    cognite_client.data_modeling.spaces.delete(spaces=[space_id])
    
@pytest.fixture(scope="function")
def test_model(cognite_client: CogniteClient, test_space: Space):
    test_dml = (RESOURCES / "movie_model.graphql").read_text()
    movie_id = DataModelId(space=test_space.space, external_id="Movie", version="1")
    created = cognite_client.data_modeling.graphql.apply_dml(
        id=movie_id, dml=test_dml, name="Movie Model", description="The Movie Model used in Integration Tests"
    )
    models = cognite_client.data_modeling.data_models.retrieve(created.as_id(), inline_views=True)
    yield models.latest_version()
    cognite_client.data_modeling.data_models.delete(ids=[movie_id])
    views = models.data[0].views
    for view in views: # Views and containers need to be deleted so the space can be deleted
        cognite_client.data_modeling.views.delete((test_space.space, view.external_id, view.version))
        cognite_client.data_modeling.containers.delete((test_space.space, view.external_id))

@pytest.fixture(scope="function")
def edge_table_path(test_space: Space, azure_credential: DefaultAzureCredential):
    edge_table_path = lakehouse_table_name(test_space.space + "_edges")
    yield edge_table_path
    try:
        delta_table = get_ts_delta_table(azure_credential, edge_table_path)
        delta_table.delete()
    except TableNotFoundError:
        print(f"Table not found {edge_table_path}")

@pytest.fixture(scope="function")
def instance_table_paths(test_model: DataModel[View], azure_credential: DefaultAzureCredential):
    instance_table_paths = []
    for view in test_model.views:
        instance_table_paths.append(lakehouse_table_name(test_model.space + "_" + view.external_id))
    yield instance_table_paths
    for path in instance_table_paths:
        try:
            delta_table = get_ts_delta_table(azure_credential, path)
            delta_table.delete()
        except TableNotFoundError:
            print(f"Table not found {path}")

@pytest.fixture(scope="function")
def example_actor():
    return {
        "external_id": "arnold_schwarzenegger",
        "Person": {"name": "Arnold Schwarzenegger", "birthYear": 1947}, 
        "Actor": {"wonOscar": False}
    }

@pytest.fixture(scope="function")
def example_movie():
    return {
        "external_id": "terminator",
        "Movie": {"title": "Terminator", "releaseYear": 1984}
    }

@pytest.fixture(scope="function")
def example_edge_1():
    return {
        "external_id": "relation:arnold_schwarzenegger:terminator",
        "start_node": "arnold_schwarzenegger",
        "end_node": "terminator"
    }

@pytest.fixture(scope="function")
def example_edge_2():
    return {
        "external_id": "relation:terminator:arnold_schwarzenegger",
        "start_node": "terminator",
        "end_node": "arnold_schwarzenegger"
    }

@pytest.fixture(scope="function")
def node_list(test_model: DataModel[View], example_actor: dict, example_movie: dict) -> list[NodeApply]:
    actor_view = [view for view in test_model.views if view.external_id == "Actor"][0]
    person_view = [view for view in test_model.views if view.external_id == "Person"][0]
    movie_view = [view for view in test_model.views if view.external_id == "Movie"][0]
    return [create_actor(test_model.space, example_actor, person_view, actor_view), create_movie(test_model.space, example_movie, movie_view)]

@pytest.fixture(scope="function")
def edge_list(test_model: DataModel[View], example_edge_1: dict, example_edge_2: dict) -> list[EdgeApply]:
    actor_view = [view for view in test_model.views if view.external_id == "Actor"][0]
    actor_type = cast(SingleHopConnectionDefinition, actor_view.properties["movies"]).type.external_id
    movie_view = [view for view in test_model.views if view.external_id == "Movie"][0]
    movie_type = cast(SingleHopConnectionDefinition, movie_view.properties["actors"]).type.external_id
    return [create_edge(test_model.space, actor_type, example_edge_1), create_edge(test_model.space, movie_type, example_edge_2)]