from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import EdgeApply, NodeOrEdgeData, NodeApply, ViewId, NodeApplyList, EdgeApplyList, InstancesApplyResult, View, DataModel


def create_node_or_edge_data(view: View, data: dict):
    return NodeOrEdgeData(
        ViewId(view.space, view.external_id, view.version),
        data
    )

def create_actor(space_id: str, actor_info: dict, person_view: View, actor_view: View):
    return NodeApply(
        space=space_id,
        external_id=actor_info["external_id"],
        sources=[
            create_node_or_edge_data(person_view, actor_info["Person"]),
            create_node_or_edge_data(actor_view, actor_info["Actor"])
        ]
    )

def create_movie(space_id: str, movie_info: dict, movie_view: View):
    return NodeApply(
        space=space_id,
        external_id=movie_info["external_id"],
        sources=[
            create_node_or_edge_data(movie_view, movie_info["Movie"])
        ]
    )

def create_edge(space_id: str, type: str, edge: dict) -> EdgeApply:
    return EdgeApply(
        space=space_id,
        external_id=edge["external_id"],
        type=(space_id, type),
        start_node=(space_id, edge["start_node"]),
        end_node=(space_id, edge["end_node"])
    )

def create_data_modeling_instances(node_list: list[NodeApply], edge_list: list[EdgeApply], client: CogniteClient) -> InstancesApplyResult:
    res = client.data_modeling.instances.apply(nodes=node_list, edges=edge_list)
    return res