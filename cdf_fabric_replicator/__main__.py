from cognite.extractorutils.base import CancellationToken

from cognite.extractorutils import Extractor
from cognite.extractorutils.metrics import safe_get

from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config
from cdf_fabric_replicator.time_series import TimeSeriesReplicator
from cdf_fabric_replicator.data_modeling import DataModelingReplicator

from cdf_fabric_replicator.metrics import Metrics
import threading

def main() -> None:
    stop_event = CancellationToken()

    with TimeSeriesReplicator(metrics=safe_get(Metrics), stop_event=stop_event) as ts_replicator:
        ts_worker = threading.Thread(target=ts_replicator.run)
        ts_worker.start()
        ts_worker.join()

    with DataModelingReplicator(metrics=safe_get(Metrics), stop_event=stop_event) as dm_replicator:
        dm_worker = threading.Thread(target=dm_replicator.run)
        dm_worker.start()
        dm_worker.join()

if __name__ == "__main__":
    main()
