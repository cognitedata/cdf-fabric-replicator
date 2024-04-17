import pytest

from collections import UserDict

from cdf_fabric_replicator.data_modeling import DataModelingReplicator
from cdf_fabric_replicator.config import Config, DataModelingConfig

from cognite.client.data_classes.data_modeling.instances import Node, Edge
from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.query import (
    QueryResult,
    NodeListWithCursor,
    EdgeListWithCursor,
)

from cognite.extractorutils.metrics import BaseMetrics
from cognite.extractorutils.base import CancellationToken


@pytest.fixture
def view_id():
    yield ViewId(space="test_space", external_id="test_view", version=1)


@pytest.fixture
def input_data_properties(view_id):
    property = UserDict()
    property_content = UserDict()
    property_content["prop1"] = "value1"
    property[view_id] = property_content
    yield property


@pytest.fixture
def input_data_types():
    yield {
        "node_type": DirectRelationReference(
            space="test_space", external_id="test_type_node"
        ),
        "edge_type": DirectRelationReference(
            space="test_space", external_id="test_type_edge"
        ),
    }


@pytest.fixture
def input_data_node_ref():
    yield {
        "start_node": DirectRelationReference(space="test_space", external_id="id1"),
        "end_node": DirectRelationReference(space="test_space", external_id="id2"),
    }


@pytest.fixture
def input_data_nodes(input_data_properties, input_data_types):
    type = input_data_types["node_type"]
    yield [
        Node(
            space="test_space",
            external_id="id1",
            version=1,
            last_updated_time=12345600,
            created_time=12345600,
            deleted_time=12378900,
            type=type,
            properties=input_data_properties,
        ),
        Node(
            space="test_space",
            external_id="id2",
            version=1,
            last_updated_time=12345600,
            created_time=12345600,
            deleted_time=None,
            type=type,
            properties=input_data_properties,
        ),
    ]


@pytest.fixture
def input_data_edges(input_data_properties, input_data_types, input_data_node_ref):
    type = input_data_types["edge_type"]
    start_node = input_data_node_ref["start_node"]
    end_node = input_data_node_ref["end_node"]
    yield [
        Edge(
            space="test_space",
            external_id="id1",
            version=1,
            last_updated_time=12345600,
            created_time=12345600,
            deleted_time=12378900,
            start_node=start_node,
            end_node=end_node,
            type=type,
            properties=input_data_properties,
        ),
        Edge(
            space="test_space",
            external_id="id2",
            version=1,
            last_updated_time=12345600,
            created_time=12345600,
            deleted_time=None,
            start_node=start_node,
            end_node=end_node,
            type=type,
            properties=input_data_properties,
        ),
    ]


@pytest.fixture
def query_result_nodes(input_data_nodes):
    query_result = QueryResult(
        nodes=NodeListWithCursor(resources=input_data_nodes, cursor=None)
    )
    yield query_result


@pytest.fixture
def query_result_edges(input_data_edges):
    yield QueryResult(edges=EdgeListWithCursor(resources=input_data_edges, cursor=None))


@pytest.fixture
def expected_node_instance():
    yield {
        "test_space_test_view": [
            {
                "space": "test_space",
                "instanceType": "node",
                "externalId": "id1",
                "version": 1,
                "lastUpdatedTime": 12345600,
                "createdTime": 12345600,
                "prop1": "value1",
            },
            {
                "space": "test_space",
                "instanceType": "node",
                "externalId": "id2",
                "version": 1,
                "lastUpdatedTime": 12345600,
                "createdTime": 12345600,
                "prop1": "value1",
            },
        ]
    }


@pytest.fixture
def expected_edge_instance():
    yield {
        "test_space_edges": [
            {
                "space": "test_space",
                "instanceType": "edge",
                "externalId": "id1",
                "version": 1,
                "lastUpdatedTime": 12345600,
                "createdTime": 12345600,
                "prop1": "value1",
                "startNode": {"space": "test_space", "externalId": "id1"},
                "endNode": {"space": "test_space", "externalId": "id2"},
            },
            {
                "space": "test_space",
                "instanceType": "edge",
                "externalId": "id2",
                "version": 1,
                "lastUpdatedTime": 12345600,
                "createdTime": 12345600,
                "prop1": "value1",
                "startNode": {"space": "test_space", "externalId": "id1"},
                "endNode": {"space": "test_space", "externalId": "id2"},
            },
        ]
    }


@pytest.fixture
def query_result_empty():
    yield QueryResult()


@pytest.fixture
def mock_write_deltalake(mocker):
    yield mocker.patch(
        "cdf_fabric_replicator.data_modeling.DataModelingReplicator.write_instances_to_lakehouse_tables",
        return_value=None,
    )


@pytest.fixture
def lakehouse_abfss_prefix():
    yield "test_abfss_prefix"


@pytest.fixture
def replicator_config(lakehouse_abfss_prefix):
    yield Config(
        type=None,
        cognite=None,
        version="0.1.0",
        logger=None,
        extractor=None,
        subscriptions=None,
        event=None,
        data_modeling=DataModelingConfig(
            space="test_space", lakehouse_abfss_prefix=lakehouse_abfss_prefix
        ),
        source=None,
        destination=None,
    )


class TestDataModelingReplicator:
    metrics = BaseMetrics(
        extractor_name="test_dm_duplicator", extractor_version="1.0.0"
    )
    replicator = DataModelingReplicator(metrics=metrics, stop_event=CancellationToken())

    def test_get_instances_null(self, query_result_empty):
        instances = self.replicator.get_instances(query_result_empty, is_edge=False)
        assert len(instances) == 0

    def test_get_instances_nodes(self, query_result_nodes, expected_node_instance):
        expected_nodes = expected_node_instance
        actual_nodes = self.replicator.get_instances(query_result_nodes, is_edge=False)
        assert actual_nodes == expected_nodes

    def test_get_instances_edges(self, query_result_edges, expected_edge_instance):
        expected_edges = expected_edge_instance
        actual_edges = self.replicator.get_instances(query_result_edges, is_edge=True)
        assert actual_edges == expected_edges

    def test_send_to_lakehouse_null(
        self, query_result_empty, mock_write_deltalake, replicator_config
    ):
        self.replicator.send_to_lakehouse(
            data_model_config=replicator_config.data_modeling,
            state_id="test_state_id",
            result=query_result_empty,
        )
        mock_write_deltalake.assert_not_called()

    def test_send_to_lakehouse_nodes(
        self,
        query_result_nodes,
        mock_write_deltalake,
        replicator_config,
        expected_node_instance,
        lakehouse_abfss_prefix,
    ):
        self.replicator.send_to_lakehouse(
            data_model_config=replicator_config.data_modeling,
            state_id="test_state_id",
            result=query_result_nodes,
        )
        mock_write_deltalake.assert_called_once()
        mock_write_deltalake.assert_called_with(
            expected_node_instance, lakehouse_abfss_prefix
        )

    def test_send_to_lakehouse_edges(
        self,
        query_result_edges,
        mock_write_deltalake,
        replicator_config,
        expected_edge_instance,
        lakehouse_abfss_prefix,
    ):
        self.replicator.send_to_lakehouse(
            data_model_config=replicator_config.data_modeling,
            state_id="test_state_id",
            result=query_result_edges,
        )
        mock_write_deltalake.assert_called_once()
        mock_write_deltalake.assert_called_with(
            expected_edge_instance, lakehouse_abfss_prefix
        )
