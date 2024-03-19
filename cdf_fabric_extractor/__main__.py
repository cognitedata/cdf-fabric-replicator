from cognite.extractorutils.base import CancellationToken

from cdf_fabric_replicator.extractor import CdfFabricExtractor


def main() -> None:
    stop_event = CancellationToken()

    with CdfFabricExtractor(stop_event=stop_event) as extractor:
        extractor.run()


if __name__ == "__main__":
    main()
