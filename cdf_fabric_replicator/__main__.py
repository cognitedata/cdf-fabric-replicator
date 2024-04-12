from cognite.extractorutils.base import CancellationToken

from cognite.extractorutils.metrics import safe_get

from cdf_fabric_replicator.time_series import TimeSeriesReplicator
from cdf_fabric_replicator.data_modeling import DataModelingReplicator
from cdf_fabric_replicator.extractor import CdfFabricExtractor
from cdf_fabric_replicator.event import EventsReplicator

from cdf_fabric_replicator.metrics import Metrics
import threading


def main() -> None:
    stop_event = CancellationToken()

    with EventsReplicator(
        metrics=safe_get(Metrics), stop_event=stop_event
    ) as event_replicator:
        event_worker = threading.Thread(target=event_replicator.run)
        event_worker.start()

    with TimeSeriesReplicator(
        metrics=safe_get(Metrics), stop_event=stop_event
    ) as ts_replicator:
        ts_worker = threading.Thread(target=ts_replicator.run)
        ts_worker.start()

    with CdfFabricExtractor(stop_event=stop_event) as extractor:
        extractor_worker = threading.Thread(target=extractor.run)
        extractor_worker.start()

    with DataModelingReplicator(
        metrics=safe_get(Metrics), stop_event=stop_event
    ) as dm_replicator:
        dm_worker = threading.Thread(target=dm_replicator.run)
        dm_worker.start()
        dm_worker.join()


if __name__ == "__main__":
    main()
