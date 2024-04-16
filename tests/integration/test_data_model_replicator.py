import os
import pytest
from unittest.mock import Mock
from azure.identity import DefaultAzureCredential
from cognite.client import CogniteClient
from pathlib import Path
from cognite.client.data_classes.data_modeling import Space, SpaceApply, DataModel, View
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.metrics import safe_get
from cdf_fabric_replicator.metrics import Metrics
from cdf_fabric_replicator.data_modeling import DataModelingReplicator
from tests.integration.integration_steps.cdf_steps import (
    apply_data_model_instances_in_cdf,
)
from tests.integration.integration_steps.service_steps import run_data_model_sync
from tests.integration.integration_steps.fabric_steps import (
    delete_delta_table_data,
    lakehouse_table_name,
    assert_data_model_in_fabric,
    assert_data_model_update,
)
from integration_steps.data_model_generation import Node, Edge, create_node, create_edge

RESOURCES = Path(__file__).parent / "resources"


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


# Test for data model sync service
def test_data_model_sync_service_creation(
    edge_table_path, instance_table_paths, node_list, edge_list, cognite_client
):
    # Create a data model in CDF
    apply_data_model_instances_in_cdf(node_list, edge_list, cognite_client)
    # Run data model sync service between CDF and Fabric
    run_data_model_sync()
    # Assert the data model is populated in a Fabric lakehouse
    assert_data_model_in_fabric()


def test_data_model_sync_service_update(
    updated_node_list,
    edge_table_path,
    instance_table_paths,
    node_list,
    edge_list,
    cognite_client,
):
    # Apply instances and run data model sync service between CDF and Fabric
    apply_data_model_instances_in_cdf(node_list, edge_list, cognite_client)
    run_data_model_sync()
    # Update instances in CDF and run data model sync
    apply_data_model_instances_in_cdf(updated_node_list, [], cognite_client)
    run_data_model_sync()
    # Assert the data model changes including versions and last updated timestamps are propagated to a Fabric lakehouse
    assert_data_model_update()
