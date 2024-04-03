import pandas as pd
from pandas import DataFrame
from datetime import datetime
from time import sleep
from typing import List
from cognite.client import CogniteClient
from cognite.client.data_classes import Datapoint, TimeSeries, TimeSeriesWrite
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client.data_classes import DataPointSubscriptionWrite, DatapointSubscription
from cognite.client.data_classes.data_modeling import (
    Space, 
    DataModel,
    View,
    NodeApply,
    EdgeApply
)
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cdf_fabric_replicator.config import SubscriptionsConfig

TIMESTAMP_COLUMN = "timestamp"

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

    try:
        cognite_client.time_series.create(time_series_write_list)
    except Exception as e:
        pass #print(f"Error creating time series: {e}")

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

def apply_data_model_instances_in_cdf(node_list: list[NodeApply], edge_list: list[EdgeApply], cognite_client: CogniteClient):
    # Create data model instances in CDF
    return cognite_client.data_modeling.instances.apply(nodes=node_list, edges=edge_list)

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


def cdf_timeseries_contain_expected_timeseries_ids(
    expected_timeseries: list[str], retrieved_timeseries_ids: list[str]
) -> bool:
    return all(
        any(ts == retrieved_timeseries_id for retrieved_timeseries_id in retrieved_timeseries_ids)
        for ts in expected_timeseries
    )


def assert_time_series_in_cdf_by_id(expected_timeseries: list[str], cognite_client: CogniteClient):

    for external_id in expected_timeseries:
        result = cognite_client.time_series.retrieve(external_id=external_id)
        assert result is not None


def compare_timestamps(timestamp1: datetime, timestamp2: datetime) -> bool:
    timestamp1_str = timestamp1.strftime("%Y-%m-%d %H:%M:%S")
    timestamp2_str = timestamp2.strftime("%Y-%m-%d %H:%M:%S")
    return timestamp1_str == timestamp2_str


def cdf_datapoints_contain_expected_datapoints(
    expected_data_list: list[tuple[str, str]], retrieved_data_point_tuple: list[tuple[str, str]]
) -> bool:
    print(f"Expected data: {expected_data_list}")
    print(f"Retrieved data: {retrieved_data_point_tuple}")
    return all(
        any(
            compare_timestamps(expected_timestamp, timestamp) and expected_value == value
            for timestamp, value in retrieved_data_point_tuple
        )
        for expected_timestamp, expected_value in expected_data_list
    )


def assert_data_points_in_cdf(
    external_id: str, expected_data_points: list[tuple[str, str]], cognite_client: CogniteClient
):
    result = cognite_client.time_series.data.retrieve_dataframe(external_id=external_id)

    result.reset_index(inplace=True)
    result.rename(columns={"index": "timestamp", external_id: "value"}, inplace=True)
    result[TIMESTAMP_COLUMN] = pd.to_datetime(result[TIMESTAMP_COLUMN])
    result[TIMESTAMP_COLUMN] = result[TIMESTAMP_COLUMN].dt.round("s")

    assert cdf_datapoints_contain_expected_datapoints(
        expected_data_points, [(row.iloc[0], row.iloc[1]) for _, row in result.iterrows()]
    )


def assert_data_points_df_in_cdf(external_id: str, data_points: DataFrame, cognite_client: CogniteClient):
    data_points_list = []
    for _, row in data_points.iterrows():
        data_point = (row.iloc[1], row.iloc[2])

        data_points_list.append(data_point)
    assert_data_points_in_cdf(
        external_id=external_id, expected_data_points=data_points_list, cognite_client=cognite_client
    )


def remove_time_series_data(list_of_time_series: list[TimeSeries], cognite_client: CogniteClient):
    for time_series in list_of_time_series:
        try:
            cognite_client.time_series.delete(external_id=time_series.external_id)
        except CogniteNotFoundError:
            print(f'time series {time_series.external_id} not found in CDF')
    sleep(5)


def remove_subscriptions(sub_name: str, cognite_client: CogniteClient):
    try:
        cognite_client.time_series.subscriptions.delete(sub_name)
    except CogniteNotFoundError:
        print(f'subscription {sub_name} not found in CDF')

def delete_state_store_in_cdf(subscriptions: List[SubscriptionsConfig], database: str, table: str, cognite_client: CogniteClient):
    for sub in subscriptions:
        for i in range(len(sub.partitions)):
            statename = f"{sub.external_id}_{i}"
            row = cognite_client.raw.rows.retrieve(database, table, statename)
            if row is not None:
                cognite_client.raw.rows.delete(database, table, statename)

    all_rows = cognite_client.raw.rows.list(database, table, limit=1)
    if len(all_rows) == 0:
        cognite_client.raw.tables.delete(database, table)

def assert_state_store_in_cdf(subscriptions: List[SubscriptionsConfig], database: str, table: str, cognite_client: CogniteClient):
    for sub in subscriptions:
        for i in range(len(sub.partitions)):
            statename = f"{sub.external_id}_{i}"
            row = cognite_client.raw.rows.retrieve(database, table, statename)

            assert row is not None
            assert row.columns["high"] is not None