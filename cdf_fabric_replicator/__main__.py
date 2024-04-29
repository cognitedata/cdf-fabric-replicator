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
    worker_list = []

    # with EventsReplicator(
    #     metrics=safe_get(Metrics), stop_event=stop_event
    # ) as event_replicator:
    #     worker_list.append(threading.Thread(target=event_replicator.run))

    # with TimeSeriesReplicator(
    #     metrics=safe_get(Metrics), stop_event=stop_event
    # ) as ts_replicator:
    #     worker_list.append(threading.Thread(target=ts_replicator.run))

    # with CdfFabricExtractor(stop_event=stop_event) as extractor:
    #     worker_list.append(threading.Thread(target=extractor.run))

    # with DataModelingReplicator(
    #     metrics=safe_get(Metrics), stop_event=stop_event
    # ) as dm_replicator:
    #     worker_list.append(threading.Thread(target=dm_replicator.run))

    for worker in worker_list:
        worker.start()

    for worker in worker_list:
        worker.join()


if __name__ == "__main__":
    main()
