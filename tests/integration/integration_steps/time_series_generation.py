from datetime import datetime, timezone, timedelta
import random
from cognite.client.data_classes import Datapoint, TimeSeries
from dataclasses import dataclass


@dataclass
class TimeSeriesGeneratorArgs:
    external_ids: list[str]
    num_data_points_per_time_series: int


def generate_datapoints(
    num_points: int, days_ago_for_time_range_start=2
) -> list[Datapoint]:
    if num_points <= 0:
        raise ValueError("Number of data points must be greater than 0")
    datapoints = []
    current_time = datetime.now(timezone.utc) - timedelta(
        days=days_ago_for_time_range_start
    )

    for _ in range(num_points):
        timestamp = current_time
        value = random.uniform(0, 50)
        datapoint = Datapoint(timestamp=timestamp, value=value)
        datapoints.append(datapoint)
        current_time += timedelta(seconds=60)

    return datapoints


def generate_timeseries_set(
    generation_args: TimeSeriesGeneratorArgs,
) -> list[TimeSeries]:
    return [
        generate_timeseries(
            external_id, generation_args.num_data_points_per_time_series
        )
        for external_id in generation_args.external_ids
    ]


def generate_timeseries(external_id: str, num_data_points: int) -> TimeSeries:
    timeseries = TimeSeries(
        external_id=external_id,
        name=f"{external_id}: testing historical values",
        is_string=False,
        metadata={"source": "carbon-sdk"},
        is_step=False,
        security_categories=[],
    )

    timeseries.datapoints = generate_datapoints(num_points=num_data_points)

    return timeseries


def generate_raw_timeseries_set(generation_args: TimeSeriesGeneratorArgs) -> list[dict]:
    flattened_timeseries = []
    for external_id in generation_args.external_ids:
        timeseries = generate_raw_timeseries(
            external_id, generation_args.num_data_points_per_time_series
        )
        for datapoint in timeseries["datapoints"]:
            flattened_timeseries.append(
                {
                    "externalId": external_id,
                    "timestamp": datapoint["timestamp"],
                    "value": datapoint["value"],
                }
            )
    return flattened_timeseries


def generate_raw_timeseries(external_id: str, num_data_points: int) -> dict:
    timeseries = {
        "externalId": external_id,
        "datapoints": generate_raw_datapoints(num_points=num_data_points),
    }

    return timeseries


def generate_raw_datapoints(
    num_points: int, days_ago_for_time_range_start=2
) -> list[dict]:
    if num_points <= 0:
        raise ValueError("Number of data points must be greater than 0")
    datapoints = []
    current_time = datetime.now(timezone.utc) - timedelta(
        days=days_ago_for_time_range_start
    )

    for _ in range(num_points):
        data_point = {"timestamp": current_time, "value": random.uniform(0, 50)}

        datapoints.append(data_point)
        current_time += timedelta(seconds=60)

    return datapoints
