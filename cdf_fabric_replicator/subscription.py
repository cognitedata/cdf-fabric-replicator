import logging

from cognite.client.data_classes import DataPointSubscriptionWrite
from cognite.client.data_classes import filters as flt
from cognite.client.data_classes.time_series import TimeSeriesProperty
from cognite.client import CogniteClient

def create_subscription(
    cognite_client: CogniteClient, external_id: str, name: str, num_partitions: int
) -> None:
    logging.debug(f"Subscription {external_id} not found. Creating subscription...")

    sub = DataPointSubscriptionWrite(
        external_id=external_id,
        name=name,
        partition_count=num_partitions,
        filter=flt.Exists(TimeSeriesProperty.external_id),
    )
    cognite_client.time_series.subscriptions.create(sub)
    logging.debug(f"Subscription successfully {external_id} created.")
