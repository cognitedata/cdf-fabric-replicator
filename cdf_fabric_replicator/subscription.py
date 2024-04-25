import logging

from typing import List
from cognite.client.data_classes import DataPointSubscriptionWrite
from cognite.client.data_classes import filters as flt
from cognite.client.data_classes.time_series import TimeSeriesProperty
from cognite.client import CogniteClient
from cdf_fabric_replicator.config import SubscriptionsConfig

def autocreate_subscription(
    subscriptions: List[SubscriptionsConfig], cognite_client: CogniteClient, name: str, num_partitions: int
) -> None:
    first_subscription = subscriptions[0]

    # If only 1 subscription is defined and it's only partition is 0, then create the subscription if it doesn't already exist
    if len(subscriptions) == 1 and first_subscription.partitions == [0]:
        if cognite_client.time_series.subscriptions.retrieve(external_id=first_subscription.external_id) is None:
            create_subscription(
                cognite_client, 
                first_subscription.external_id, 
                name, 
                num_partitions
            )

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
