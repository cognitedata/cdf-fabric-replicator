from threading import Event

from cognite.extractorutils import Extractor
from cognite.extractorutils.metrics import safe_get

from cdf_ts_replicator import __version__
from cdf_ts_replicator.config import Config
from cdf_ts_replicator.extractor import TimeSeriesReplicator
from cdf_ts_replicator.metrics import Metrics


def main() -> None:
    stop_event = Event()

    with TimeSeriesReplicator(metrics=safe_get(Metrics), stop_event=stop_event) as extractor:
        extractor.run()


if __name__ == "__main__":
    main()
