import os
import pytest
import yaml
from unittest.mock import Mock
from time import sleep
from azure.identity import DefaultAzureCredential
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from pathlib import Path
from cognite.client.data_classes.data_modeling import Space, SpaceApply, DataModel, View
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.metrics import safe_get
from cdf_fabric_replicator.metrics import Metrics
from cdf_fabric_replicator.time_series import TimeSeriesReplicator
from cdf_fabric_replicator.extractor import CdfFabricExtractor
from cdf_fabric_replicator.data_modeling import DataModelingReplicator
from dotenv import load_dotenv
from tests.integration.integration_steps.cdf_steps import (
    delete_state_store_in_cdf,
    remove_time_series_data,
    push_time_series_to_cdf,
    create_subscription_in_cdf,
    remove_subscriptions,
)
from tests.integration.integration_steps.fabric_steps import (
    delete_delta_table_data,
    write_timeseries_data_to_fabric,
    remove_time_series_data_from_fabric,
)
from tests.integration.integration_steps.time_series_generation import (
    generate_timeseries_set,
    generate_raw_timeseries_set,
    generate_timeseries,
)
from integration_steps.data_model_generation import Node, Edge, create_node, create_edge

import pandas as pd

load_dotenv()
RESOURCES = Path(__file__).parent / "resources"


def lakehouse_table_name(table_name: str):
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
    replicator.cognite_client = replicator.config.cognite.get_cognite_client(
        replicator.name
    )
    replicator.logger = Mock()
    yield replicator
    try:
        os.remove("states.json")
    except FileNotFoundError:
        pass


@pytest.fixture(scope="session")
def test_extractor():
    stop_event = CancellationToken()
    exatractor = CdfFabricExtractor(stop_event=stop_event, name="conftest")
    exatractor._initial_load_config(override_path=os.environ["TEST_CONFIG_PATH"])
    exatractor.client = exatractor.config.cognite.get_cognite_client(exatractor.name)
    exatractor.cognite_client = exatractor.config.cognite.get_cognite_client(
        exatractor.name
    )
    exatractor._load_state_store()
    exatractor.logger = Mock()
    yield exatractor
    try:
        os.remove("states.json")
    except FileNotFoundError:
        pass


@pytest.fixture(scope="function")
def test_data_modeling_replicator():
    stop_event = CancellationToken()
    replicator = DataModelingReplicator(
        metrics=safe_get(Metrics), stop_event=stop_event
    )
    replicator._initial_load_config(override_path=os.environ["TEST_CONFIG_PATH"])
    replicator.cognite_client = replicator.config.cognite.get_cognite_client(
        replicator.name
    )
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
    lakehouse_timeseries_path = (
        os.environ["LAKEHOUSE_ABFSS_PREFIX"] + "/Tables/" + os.environ["DPS_TABLE_NAME"]
    )
    delete_delta_table_data(azure_credential, lakehouse_timeseries_path)
    yield lakehouse_timeseries_path
    delete_delta_table_data(azure_credential, lakehouse_timeseries_path)


@pytest.fixture()
def time_series(request, cognite_client):
    sub_name = "testSubscription"
    timeseries_set = generate_timeseries_set(request.param)
    remove_time_series_data(timeseries_set, cognite_client)
    remove_subscriptions(sub_name, cognite_client)
    push_time_series_to_cdf(timeseries_set, cognite_client)
    create_subscription_in_cdf(timeseries_set, sub_name, cognite_client)
    sleep(5)
    yield timeseries_set
    remove_time_series_data(timeseries_set, cognite_client)
    remove_subscriptions(sub_name, cognite_client)


@pytest.fixture(scope="session")
def test_space(test_config, cognite_client: CogniteClient):
    space_id = test_config["data_modeling"][0]["space"]
    space = cognite_client.data_modeling.spaces.retrieve(space_id)
    if space is None:
        new_space = SpaceApply(
            space_id,
            name="Integration Test Space",
            description="The space used for integration tests.",
        )
        space = cognite_client.data_modeling.spaces.apply(new_space)
    else:  # Ensure there aren't existing data models
        data_model_list = cognite_client.data_modeling.data_models.list(space=space_id)
        assert (
            len(data_model_list) == 0
        ), "Space should not have existing data models, please remove models/views/containers before testing"
    yield space
    cognite_client.data_modeling.spaces.delete(spaces=[space_id])


@pytest.fixture(scope="function")
def test_model(cognite_client: CogniteClient, test_space: Space):
    test_dml = (RESOURCES / "movie_model.graphql").read_text()
    movie_id = DataModelId(space=test_space.space, external_id="Movie", version="1")
    created = cognite_client.data_modeling.graphql.apply_dml(
        id=movie_id,
        dml=test_dml,
        name="Movie Model",
        description="The Movie Model used in Integration Tests",
    )
    models = cognite_client.data_modeling.data_models.retrieve(
        created.as_id(), inline_views=True
    )
    yield models.latest_version()
    cognite_client.data_modeling.data_models.delete(ids=[movie_id])
    views = models.data[0].views
    for (
        view
    ) in views:  # Views and containers need to be deleted so the space can be deleted
        cognite_client.data_modeling.views.delete(
            (test_space.space, view.external_id, view.version)
        )
        cognite_client.data_modeling.containers.delete(
            (test_space.space, view.external_id)
        )


@pytest.fixture(scope="function")
def edge_table_path(test_space: Space, azure_credential: DefaultAzureCredential):
    edge_table_path = lakehouse_table_name(test_space.space + "_edges")
    delete_delta_table_data(azure_credential, edge_table_path)
    yield edge_table_path
    delete_delta_table_data(azure_credential, edge_table_path)


@pytest.fixture(scope="function")
def instance_table_paths(
    test_model: DataModel[View], azure_credential: DefaultAzureCredential
):
    instance_table_paths = []
    for view in test_model.views:
        instance_table_paths.append(
            lakehouse_table_name(test_model.space + "_" + view.external_id)
        )
        delete_delta_table_data(azure_credential, instance_table_paths[-1])
    yield instance_table_paths
    for path in instance_table_paths:
        delete_delta_table_data(azure_credential, path)


@pytest.fixture(scope="function")
def example_actor():
    return Node(
        "arnold_schwarzenegger",
        "Actor",
        {
            "Actor": {"wonOscar": False},
            "Person": {"name": "Arnold Schwarzenegger", "birthYear": 1947},
        },
    )


@pytest.fixture(scope="function")
def updated_actor():
    return Node("arnold_schwarzenegger", "Actor", {"Actor": {"wonOscar": True}})


@pytest.fixture(scope="function")
def example_movie():
    return Node(
        "terminator", "Movie", {"Movie": {"title": "Terminator", "releaseYear": 1984}}
    )


@pytest.fixture(scope="function")
def example_edge_actor_to_movie(example_actor, example_movie):
    return Edge(
        "relation:arnold_schwarzenegger:terminator",
        "movies",
        example_actor,
        example_movie,
    )


@pytest.fixture(scope="function")
def example_edge_movie_to_actor(example_actor, example_movie):
    return Edge(
        "relation:terminator:arnold_schwarzenegger",
        "actors",
        example_movie,
        example_actor,
    )


@pytest.fixture(scope="function")
def node_list(
    test_model: DataModel[View],
    example_actor: Node,
    example_movie: Node,
    cognite_client: CogniteClient,
):
    yield [
        create_node(test_model.space, example_actor, test_model),
        create_node(test_model.space, example_movie, test_model),
    ]
    cognite_client.data_modeling.instances.delete(
        nodes=[
            (test_model.space, example_actor.external_id),
            (test_model.space, example_movie.external_id),
        ]
    )


@pytest.fixture(scope="function")
def updated_node_list(
    test_model: DataModel[View], updated_actor: Node, cognite_client: CogniteClient
):
    yield [create_node(test_model.space, updated_actor, test_model)]
    cognite_client.data_modeling.instances.delete(
        nodes=(test_model.space, updated_actor.external_id)
    )


@pytest.fixture(scope="function")
def edge_list(
    test_model: DataModel[View],
    example_edge_actor_to_movie: Edge,
    example_edge_movie_to_actor: Edge,
    cognite_client,
):
    edge_list = [
        create_edge(test_model.space, example_edge_actor_to_movie, test_model),
        create_edge(test_model.space, example_edge_movie_to_actor, test_model),
    ]
    yield edge_list
    cognite_client.data_modeling.instances.delete(
        edges=[
            (test_model.space, example_edge_actor_to_movie.external_id),
            (test_model.space, example_edge_movie_to_actor.external_id),
        ]
    )
    cognite_client.data_modeling.instances.delete(
        nodes=[(test_model.space, edge.type.external_id) for edge in edge_list]
    )


@pytest.fixture()
def remote_state_store(cognite_client, test_replicator):
    test_replicator._load_state_store()
    state_store = test_replicator.state_store
    yield state_store
    delete_state_store_in_cdf(
        test_replicator.config.subscriptions,
        test_replicator.config.extractor.state_store.raw.database,
        test_replicator.config.extractor.state_store.raw.table,
        cognite_client,
    )


@pytest.fixture(scope="function")
def raw_time_series(request, azure_credential, cognite_client, test_extractor):
    timeseries_set = generate_raw_timeseries_set(request.param)
    df = pd.DataFrame(timeseries_set, columns=["externalId", "timestamp", "value"])
    remove_time_series_data_from_fabric(
        azure_credential,
        test_extractor.config.source.abfss_prefix
        + "/"
        + test_extractor.config.source.raw_time_series_path,
    )
    write_timeseries_data_to_fabric(
        azure_credential,
        df,
        test_extractor.config.source.abfss_prefix
        + "/"
        + test_extractor.config.source.raw_time_series_path,
    )

    unique_external_ids = set()
    generated_timeseries = []
    for ts in timeseries_set:
        if ts["externalId"] not in unique_external_ids:
            push_time_series_to_cdf(
                [generate_timeseries(ts["externalId"], 1)], cognite_client
            )
            generated_timeseries.append(generate_timeseries(ts["externalId"], 1))
            unique_external_ids.add(ts["externalId"])
    yield df
    for ts in generated_timeseries:
        remove_time_series_data(generated_timeseries, cognite_client)

    remove_time_series_data_from_fabric(
        azure_credential,
        test_extractor.config.source.abfss_prefix
        + "/"
        + test_extractor.config.source.raw_time_series_path,
    )

    delete_state_store_in_cdf(
        test_extractor.config.subscriptions,
        test_extractor.config.extractor.state_store.raw.database,
        test_extractor.config.extractor.state_store.raw.table,
        cognite_client,
    )
