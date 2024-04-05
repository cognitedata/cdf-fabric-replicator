from dataclasses import dataclass
from typing import cast

from cognite.client.data_classes.data_modeling import (
    EdgeApply,
    NodeOrEdgeData,
    NodeApply,
    ViewId,
    View,
    DataModel,
    SingleHopConnectionDefinition,
)


@dataclass
class Node:
    external_id: str
    type: str
    source_dict: dict


@dataclass
class Edge:
    external_id: str
    property: str
    start_node: Node
    end_node: Node


def get_view_for_node(type: str, test_model: DataModel[View]) -> View:
    # Gets the view that matches the node type
    return [view for view in test_model.views if view.external_id == type][0]


def get_type_for_edge(edge: Edge, test_model: DataModel[View]) -> str:
    # Gets the type that this edge will populate from the data model, i.e. creating an edge from actor to movie will populate the "movies" field in Actor
    return cast(
        SingleHopConnectionDefinition,
        get_view_for_node(edge.start_node.type, test_model).properties[edge.property],
    ).type.external_id


def create_node_or_edge_data(view: View, data: dict):
    return NodeOrEdgeData(ViewId(view.space, view.external_id, view.version), data)


def create_node(space_id: str, node: Node, test_model: DataModel[View]):
    views = [get_view_for_node(key, test_model) for key in node.source_dict.keys()]
    return NodeApply(
        space=space_id,
        external_id=node.external_id,
        sources=[
            create_node_or_edge_data(view, node.source_dict[view.external_id])
            for view in views
        ],
    )


def create_edge(space_id: str, edge: Edge, test_model: DataModel[View]) -> EdgeApply:
    type = get_type_for_edge(edge, test_model)
    return EdgeApply(
        space=space_id,
        external_id=edge.external_id,
        type=(space_id, type),
        start_node=(space_id, edge.start_node.external_id),
        end_node=(space_id, edge.end_node.external_id),
    )
