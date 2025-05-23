from cognite.extractorutils.metrics import BaseMetrics
from prometheus_client import Counter

from cdf_fabric_replicator import __version__ as fabric_replicator_version


class Metrics(BaseMetrics):
    """
    A collection of metrics
    """

    def __init__(self) -> None:
        super(Metrics, self).__init__(
            "cdf_fabric_replicator", fabric_replicator_version
        )

        self.upserts_processed = Counter(
            "cdf_fabric_replicator_upserts_processed", "Number of upserts processed"
        )
        self.datapoints_written = Counter(
            "cdf_fabric_replicator_datapoints_written", "Datapoints written"
        )
        self.eventhub_messages_submitted = Counter(
            "cdf_fabric_replicator_eventhub_messages_submitted",
            "Messages submitted to eventhub",
        )
