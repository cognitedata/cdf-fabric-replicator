import pytest
from unittest.mock import patch, Mock

from collections import UserDict
import pyarrow as pa

from cdf_fabric_replicator.data_modeling import DataModelingReplicator
from cdf_fabric_replicator.config import Config, DataModelingConfig

from cognite.client.data_classes.data_modeling.instances import Node, Edge
from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.query import (
    Query,
    QueryResult,
    NodeListWithCursor,
    EdgeListWithCursor,
    Select,
    SourceSelector,
    NodeResultSetExpression,
    EdgeResultSetExpression,
)
from cognite.client.data_classes.filters import HasData, Equals


@pytest.fixture
def test_data_modeling_replicator():
    replicator = DataModelingReplicator(metrics=Mock(), stop_event=Mock())
    replicator.cognite_client = Mock()
    replicator.config = Mock()
    replicator.state_store = Mock()
    yield replicator


@pytest.fixture
def mock_data_modeling_config():
    yield DataModelingConfig(
        space="test_space", lakehouse_abfss_prefix="test_abfss_prefix"
    )


@pytest.fixture
def node_view_query():
    yield Query(
        with_={
            "nodes": NodeResultSetExpression(
                filter=HasData(
                    views=[
                        ViewId(
                            space="test_space",
                            external_id="test_id",
                            version="test_version",
                        )
                    ]
                )
            )
        },
        select={
            "nodes": Select(
                [
                    SourceSelector(
                        source=ViewId(
                            space="test_space",
                            external_id="test_id",
                            version="test_version",
                        ),
                        properties=["prop1", "prop2"],
                    )
                ]
            )
        },
    )


@pytest.fixture
def edge_view_query():
    yield Query(
        with_={
            "edges": EdgeResultSetExpression(
                filter=Equals(
                    ["edge", "type"], {"space": "test_space", "externalId": "test_id"}
                )
            )
        },
        select={
            "edges": Select(
                [
                    SourceSelector(
                        source=ViewId(
                            space="test_space",
                            external_id="test_id",
                            version="test_version",
                        ),
                        properties=["prop1", "prop2"],
                    )
                ]
            )
        },
    )


@pytest.fixture
def edge_query():
    yield Query(
        with_={"edges": EdgeResultSetExpression()},
        select={"edges": Select()},
    )


@pytest.fixture
def mock_view():
    yield {
        "space": "test_space",
        "externalId": "test_id",
        "version": "test_version",
        "usedFor": "node",
        "properties": {"prop1": "value1", "prop2": "value2"},
    }


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
def replicator_config(mock_data_modeling_config):
    yield Config(
        type=None,
        cognite=None,
        version="0.1.0",
        logger=None,
        extractor=None,
        subscriptions=None,
        event=None,
        data_modeling=mock_data_modeling_config,
        source=None,
        destination=None,
    )


class TestDataModelingReplicator:
    def test_run(self, mock_data_modeling_config, test_data_modeling_replicator):
        # Mock the stop_event.is_set response to only run replicator once
        test_data_modeling_replicator.stop_event.is_set.side_effect = [
            False,
            True,
        ]
        # Mock the config
        test_data_modeling_replicator.config = Mock(
            data_modeling=[mock_data_modeling_config], extractor=Mock(poll_time=1)
        )
        # Mock the process_spaces method since it's not the focus of this test
        test_data_modeling_replicator.process_spaces = Mock()
        # Call the method under test
        test_data_modeling_replicator.run()
        # Asssert that the process_spaces method was called
        test_data_modeling_replicator.process_spaces.assert_called_once()

    def test_run_no_data_modeling(self, test_data_modeling_replicator):
        # Mock the config
        test_data_modeling_replicator.config = Mock(
            data_modeling=None, extractor=Mock(poll_time=1)
        )
        # Mock the process_spaces method to assert not called
        test_data_modeling_replicator.process_spaces = Mock()
        # Call the method under test
        test_data_modeling_replicator.run()
        # Asssert that the process_spaces method was not called
        test_data_modeling_replicator.process_spaces.assert_not_called()

    def test_process_spaces(
        self, mock_data_modeling_config, test_data_modeling_replicator
    ):
        # Mock the data_modeling config
        test_data_modeling_replicator.config.data_modeling = [mock_data_modeling_config]

        # Mock the views.list response
        test_data_modeling_replicator.cognite_client.data_modeling.views.list.return_value = Mock(
            dump=Mock(
                return_value=[{"externalId": "test_id", "version": "test_version"}]
            )
        )

        # Mock the process_instances method since it's not the focus of this test
        test_data_modeling_replicator.process_instances = Mock()

        # Call the method under test
        test_data_modeling_replicator.process_spaces()

        # Assert that the process_instances method was called with the expected arguments
        test_data_modeling_replicator.process_instances.assert_any_call(
            mock_data_modeling_config,
            "state_test_space_test_id_test_version",
            {"externalId": "test_id", "version": "test_version"},
        )
        test_data_modeling_replicator.process_instances.assert_any_call(
            mock_data_modeling_config, "state_test_space_edges"
        )

    def test_process_instances_with_view(
        self, mock_data_modeling_config, mock_view, test_data_modeling_replicator
    ):
        # Mock the generate_query responses
        test_data_modeling_replicator.generate_query_based_on_view = Mock(
            return_value="test_query"
        )
        test_data_modeling_replicator.generate_query_based_on_edge = Mock()

        # Mock the write_instance_to_lakehouse method
        test_data_modeling_replicator.write_instance_to_lakehouse = Mock()

        # Call the method under test
        test_data_modeling_replicator.process_instances(
            mock_data_modeling_config,
            "state_test_space_test_id_test_version",
            mock_view,
        )

        # Assert that the generate_query_based_on_view method was called with the expected arguments
        test_data_modeling_replicator.generate_query_based_on_view.assert_called_once_with(
            mock_view
        )

        # Assert that the write_instance_to_lakehouse method was called with the expected arguments
        test_data_modeling_replicator.write_instance_to_lakehouse.assert_called_once_with(
            mock_data_modeling_config,
            "state_test_space_test_id_test_version",
            "test_query",
        )

        # Assert that the generate_query_based_on_edge method was not called
        test_data_modeling_replicator.generate_query_based_on_edge.assert_not_called()

    def test_process_instances_with_edge(
        self, mock_data_modeling_config, test_data_modeling_replicator
    ):
        # Mock the generate_query responses
        test_data_modeling_replicator.generate_query_based_on_view = Mock()
        test_data_modeling_replicator.generate_query_based_on_edge = Mock(
            return_value="test_query"
        )

        # Mock the write_instance_to_lakehouse method
        test_data_modeling_replicator.write_instance_to_lakehouse = Mock()

        # Call the method under test
        test_data_modeling_replicator.process_instances(
            mock_data_modeling_config, "state_test_space_edges"
        )

        # Assert that the generate_query_based_on_view method was not called
        test_data_modeling_replicator.generate_query_based_on_view.assert_not_called()

        # Assert that the write_instance_to_lakehouse method was called with the expected arguments
        test_data_modeling_replicator.write_instance_to_lakehouse.assert_called_once_with(
            mock_data_modeling_config, "state_test_space_edges", "test_query"
        )

        # Assert that the generate_query_based_on_edge method was called
        test_data_modeling_replicator.generate_query_based_on_edge.assert_called_once()

    def test_generate_query_based_on_view_node(
        self, node_view_query, mock_view, test_data_modeling_replicator
    ):
        # Call the method under test
        result_query = test_data_modeling_replicator.generate_query_based_on_view(
            mock_view
        )

        # Assert that the query is as expected
        assert result_query == node_view_query

    def test_generate_query_based_on_view_edge(
        self, edge_view_query, mock_view, test_data_modeling_replicator
    ):
        # Modify the view to be used for edge
        mock_view["usedFor"] = "edge"
        # Call the method under test
        result_query = test_data_modeling_replicator.generate_query_based_on_view(
            mock_view
        )

        # Assert that the query is as expected
        assert result_query == edge_view_query

    def test_generate_query_based_on_edge(
        self, edge_query, test_data_modeling_replicator
    ):
        # Call the method under test
        result_query = test_data_modeling_replicator.generate_query_based_on_edge()

        # Assert that the query is as expected
        assert result_query == edge_query

    def test_write_instance_to_lakehouse(
        self, mock_data_modeling_config, node_view_query, test_data_modeling_replicator
    ):
        test_node = Node(
            space="test_space",
            external_id="test_id",
            version="test_version",
            last_updated_time=12345600,
            created_time=12345600,
            deleted_time=12378900,
            type=None,
            properties={"prop1": "value1", "prop2": "value2"},
        )
        test_data_modeling_replicator.state_store.get_state.return_value = [
            None,
            '{"cursor" : "test_cursor"}',
        ]
        # Mock the get_instances method
        test_data_modeling_replicator.send_to_lakehouse = Mock()

        # Mock two responses for instance sync, first with node data second without
        test_data_modeling_replicator.cognite_client.data_modeling.instances.sync.side_effect = [
            QueryResult(
                nodes=NodeListWithCursor(
                    resources=[test_node],
                    cursor=None,
                )
            ),
            QueryResult(),
        ]

        # Call the method under test
        test_data_modeling_replicator.write_instance_to_lakehouse(
            mock_data_modeling_config,
            "state_test_space_test_id_test_version",
            node_view_query,
        )

        # Assert that the get_instances method was called with the expected arguments
        test_data_modeling_replicator.send_to_lakehouse.assert_any_call(
            data_model_config=mock_data_modeling_config,
            state_id="state_test_space_test_id_test_version",
            result=QueryResult(
                nodes=NodeListWithCursor(
                    resources=[test_node],
                    cursor={"cursor": "test_cursor"},
                )
            ),
        )

    def test_get_instances_null(
        self, query_result_empty, test_data_modeling_replicator
    ):
        instances = test_data_modeling_replicator.get_instances(
            query_result_empty, is_edge=False
        )
        assert len(instances) == 0

    def test_get_instances_nodes(
        self, query_result_nodes, expected_node_instance, test_data_modeling_replicator
    ):
        expected_nodes = expected_node_instance
        actual_nodes = test_data_modeling_replicator.get_instances(
            query_result_nodes, is_edge=False
        )
        assert actual_nodes == expected_nodes

    def test_get_instances_edges(
        self, query_result_edges, expected_edge_instance, test_data_modeling_replicator
    ):
        expected_edges = expected_edge_instance
        actual_edges = test_data_modeling_replicator.get_instances(
            query_result_edges, is_edge=True
        )
        assert actual_edges == expected_edges

    def test_send_to_lakehouse_null(
        self,
        query_result_empty,
        mock_write_deltalake,
        replicator_config,
        test_data_modeling_replicator,
    ):
        test_data_modeling_replicator.send_to_lakehouse(
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
        test_data_modeling_replicator,
    ):
        test_data_modeling_replicator.send_to_lakehouse(
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
        test_data_modeling_replicator,
    ):
        test_data_modeling_replicator.send_to_lakehouse(
            data_model_config=replicator_config.data_modeling,
            state_id="test_state_id",
            result=query_result_edges,
        )
        mock_write_deltalake.assert_called_once()
        mock_write_deltalake.assert_called_with(
            expected_edge_instance, lakehouse_abfss_prefix
        )

    @patch(
        "cdf_fabric_replicator.data_modeling.DefaultAzureCredential.get_token",
        return_value=Mock(token="test_token"),
    )
    @patch("cdf_fabric_replicator.data_modeling.write_deltalake")
    def test_write_instances_to_lakehouse_tables(
        self, mock_deltalake_write, mock_token, test_data_modeling_replicator
    ):
        test_data = [
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
        pyarrow_data = pa.Table.from_pylist(test_data)
        test_data_modeling_replicator.write_instances_to_lakehouse_tables(
            {"test_space_test_view": test_data},
            "test_abfss_prefix",
        )
        mock_token.assert_called_once()
        mock_deltalake_write.assert_called_once_with(
            "test_abfss_prefix/Tables/test_space_test_view",
            pyarrow_data,
            engine="rust",
            mode="append",
            schema_mode="merge",
            storage_options={
                "bearer_token": "test_token",
                "use_fabric_endpoint": "true",
            },
        )
