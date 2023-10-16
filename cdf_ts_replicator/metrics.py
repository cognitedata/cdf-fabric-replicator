from cognite.extractorutils.metrics import BaseMetrics
from prometheus_client import Counter, Gauge

from cdf_ts_replicator import __version__


class Metrics(BaseMetrics):
    """
    A collection of metrics
    """

    def __init__(self) -> None:
        super(Metrics, self).__init__("cdf_ts_replicator", __version__)

        self.upserts_processed = Counter("cdf_ts_replicator_upserts_processed", "Number of upserts processed")
        self.datapoints_written = Counter("cdf_ts_replicator_datapoints_written", "Datapoints written")
        self.eventhub_messages_submitted = Counter(
            "cdf_ts_replicator_eventhub_messages_submitted", "Messages submitted to eventhub"
        )
