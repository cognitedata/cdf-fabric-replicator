import pandas as pd
from datetime import datetime
from time import sleep
from cognite.client import CogniteClient
from cognite.client.data_classes import Datapoint, TimeSeries, TimeSeriesWrite
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client.data_classes import DataPointSubscriptionWrite, DatapointSubscription
from integration_steps.data_model_generation import create_data_modeling_instances
from cognite.client.data_classes.data_modeling import (
    Space, 
    SpaceApply,
    DataModel,
    View,
    NodeApply,
    EdgeApply
)
from cognite.client.data_classes.data_modeling.ids import DataModelId

def push_data_points_to_cdf(
    external_id: str, data_points: list[Datapoint], cognite_client: CogniteClient
) -> pd.DataFrame:
    df = pd.DataFrame(columns=["timestamp", "value", "externalId"])

    data_point_list = []

    for datapoint in data_points:
        data_point_list.append(
            {
                "externalId": external_id,
                "timestamp": str(datapoint.timestamp),
                "value": datapoint.value
            }
        )

    df_original = pd.DataFrame(data_point_list)
    df = df_original.pivot(index="timestamp", columns="externalId", values="value")
    df.index = pd.to_datetime(df.index)

    cognite_client.time_series.data.insert_dataframe(df)
    return df_original


def push_time_series_to_cdf(time_series_data: list[TimeSeries], cognite_client: CogniteClient) -> list[TimeSeries]:
    time_series_write_list = []

    for timeseries in time_series_data:
        time_series_write_list.append(TimeSeriesWrite(
            external_id=timeseries.external_id,
            name=timeseries.name,
            metadata=timeseries.metadata,
            is_string=timeseries.is_string,
            security_categories=timeseries.security_categories,
            is_step=timeseries.is_step,
            description=timeseries.description,
        ))

    cognite_client.time_series.create(time_series_write_list)
    return time_series_data

def push_data_to_cdf(time_series_data: list[TimeSeries], cognite_client: CogniteClient) -> dict[str, pd.DataFrame]:
    time_series_data_points_pushed = {}
    for ts in time_series_data:
        time_series_data_points_pushed[ts.external_id] = push_data_points_to_cdf(
            data_points=ts.datapoints, external_id=ts.external_id, cognite_client=cognite_client
        )
    sleep(5)
    return time_series_data_points_pushed

def create_subscription_in_cdf(time_series_data: list[TimeSeries], sub_name: str, cognite_client: CogniteClient) -> DatapointSubscription:
    ts_external_ids = [ts.external_id for ts in time_series_data]
    sub = DataPointSubscriptionWrite(sub_name, partition_count=1, time_series_ids=ts_external_ids, name="Test subscription")
    return cognite_client.time_series.subscriptions.create(sub)

def create_data_model_in_cdf(test_space: Space, test_dml: str, cognite_client: CogniteClient):
    # Create a data model in CDF
    movie_id = DataModelId(space=test_space.space, external_id="Movie", version="1")
    created = cognite_client.data_modeling.graphql.apply_dml(
        id=movie_id, dml=test_dml, name="Movie Model", description="The Movie Model used in Integration Tests"
    )
    models = cognite_client.data_modeling.data_models.retrieve(created.as_id(), inline_views=True)
    return models.latest_version()

def create_data_model_instances_in_cdf(node_list: list[NodeApply], edge_list: list[EdgeApply], data_model: DataModel[View], cognite_client: CogniteClient):
    # Create data model instances in CDF
    # edge_list = [create_actor_movie_edge(data_model.space, edge)]
    create_data_modeling_instances(node_list, edge_list, cognite_client)

def update_data_model_in_cdf():
    # Update a data model in CDF
    pass


def compare_timestamps(timestamp1: datetime, timestamp2: datetime) -> bool:
    return timestamp1.replace(microsecond=0) == timestamp2.replace(microsecond=0)

def remove_matching_data_point(data_list: list[Datapoint], timestamp: str, value: str):
    return [datapoint for datapoint in data_list if compare_timestamps(datapoint.timestamp, timestamp) and datapoint.value != value]

def remove_matching_time_series(time_series_list: list[TimeSeries], external_id: str):
    return [time_series for time_series in time_series_list if time_series.external_id != external_id]

def cdf_timeseries_contain_expected_timeseries(expected_timeseries: list[TimeSeries], retrieved_timeseries_ids: list[str]) -> bool:
    return all(
        any(
            ts.external_id == retrieved_timeseries_id
            for retrieved_timeseries_id in retrieved_timeseries_ids
        )
        for ts in expected_timeseries
    )


def cdf_datapoints_contain_expected_datapoints(expected_data_list: list[Datapoint], retrieved_data_point_tuple: list[tuple[str, str]]) -> bool:
    return all(
        any(
            compare_timestamps(data_point.timestamp, timestamp) and data_point.value == value
            for timestamp, value in retrieved_data_point_tuple
        )
        for data_point in expected_data_list
    )


def assert_data_points_in_cdf(external_id: str, expected_data_points: list[Datapoint], cognite_client: CogniteClient):
    result = cognite_client.time_series.data.retrieve_dataframe(external_id=external_id)

    assert cdf_datapoints_contain_expected_datapoints(expected_data_points, [(row[0], row[1][0]) for row in result.iterrows()])


def assert_time_series_in_cdf(expected_timeseries: list[TimeSeries], cognite_client: CogniteClient):
    result = cognite_client.time_series.list(limit=-1)

    assert cdf_timeseries_contain_expected_timeseries(expected_timeseries, [ts.external_id for ts in result])

def remove_time_series_data(list_of_time_series: list[TimeSeries], sub_name: str, cognite_client: CogniteClient):
    for time_series in list_of_time_series:
        try:
            cognite_client.time_series.delete(external_id=time_series.external_id)
        except CogniteNotFoundError:
            print(f'time series {time_series.external_id} not found in CDF')

    try:
        cognite_client.time_series.subscriptions.delete(sub_name)
    except CogniteNotFoundError:
        print(f'subscription {sub_name} not found in CDF')

def remove_data_model(data_model: DataModel[View], cognite_client: CogniteClient):
    cognite_client.data_modeling.data_models.delete((data_model.space, data_model.external_id, data_model.version))
    views = data_model.views
    for view in views: # Views and containers need to be deleted so the space can be deleted
        cognite_client.data_modeling.views.delete((data_model.space, view.external_id, view.version))
        cognite_client.data_modeling.containers.delete((data_model.space, view.external_id))
