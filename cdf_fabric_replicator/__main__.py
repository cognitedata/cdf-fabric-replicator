import logging
import threading

from cognite.extractorutils.base import CancellationToken

from cognite.extractorutils.metrics import safe_get

from cdf_fabric_replicator.time_series import TimeSeriesReplicator
from cdf_fabric_replicator.data_modeling import DataModelingReplicator
from cdf_fabric_replicator.extractor import CdfFabricExtractor
from cdf_fabric_replicator.event import EventsReplicator
from cdf_fabric_replicator.raw import RawTableReplicator
from cdf_fabric_replicator.onelake_optimizer import OnelakeOptimizer

from cdf_fabric_replicator.metrics import Metrics
from cdf_fabric_replicator.log_config import LOGGING_CONFIG


def main() -> None:
    logging.config.dictConfig(LOGGING_CONFIG)
    logging.info("Starting CDF Fabric Replicator")
    stop_event = CancellationToken()
    worker_list = []

    with EventsReplicator(
        metrics=safe_get(Metrics), stop_event=stop_event
    ) as event_replicator:
        worker_list.append(threading.Thread(target=event_replicator.run))

    with TimeSeriesReplicator(
        metrics=safe_get(Metrics), stop_event=stop_event
    ) as ts_replicator:
        worker_list.append(threading.Thread(target=ts_replicator.run))

    with CdfFabricExtractor(
        metrics=safe_get(Metrics), stop_event=stop_event
    ) as extractor:
        worker_list.append(threading.Thread(target=extractor.run))

    with DataModelingReplicator(
        metrics=safe_get(Metrics), stop_event=stop_event
    ) as dm_replicator:
        worker_list.append(threading.Thread(target=dm_replicator.run))

    with RawTableReplicator(
        metrics=safe_get(Metrics), stop_event=stop_event
    ) as raw_replicator:
        worker_list.append(threading.Thread(target=raw_replicator.run))

    with OnelakeOptimizer(
        metrics=safe_get(Metrics), stop_event=stop_event
    ) as onelake_optimizer:
        worker_list.append(threading.Thread(target=onelake_optimizer.run))

    for worker in worker_list:
        worker.start()

    for worker in worker_list:
        worker.join()


if __name__ == "__main__":
    main()
