from threading import Event

from cognite.extractorutils import Extractor
from cognite.extractorutils.metrics import safe_get

from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config
from cdf_fabric_replicator.time_series import TimeSeriesReplicator
from cdf_fabric_replicator.data_modeling import DataModelingReplicator

from cdf_fabric_replicator.metrics import Metrics
import threading

def main() -> None:
    stop_event = Event()

    with TimeSeriesReplicator(metrics=safe_get(Metrics)) as ts_replicator: #, stop_event=stop_event) as extractor:
        ts_worker = threading.Thread(target=ts_replicator.run)
        ts_worker.start()
        ts_worker.join()

    with DataModelingReplicator(metrics=safe_get(Metrics)) as dm_replicator: #, stop_event=stop_event) as extractor:
        dm_worker = threading.Thread(target=dm_replicator.run)
        dm_worker.start()
        dm_worker.join()

if __name__ == "__main__":
    main()
