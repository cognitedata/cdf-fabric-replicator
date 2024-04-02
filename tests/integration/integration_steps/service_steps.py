from cdf_fabric_replicator.time_series import TimeSeriesReplicator
from cdf_fabric_replicator.extractor import CdfFabricExtractor
from pandas import DataFrame

def run_replicator(test_replicator: TimeSeriesReplicator):
    # Processes data point subscription batches
    test_replicator.process_subscriptions()


def run_extractor(test_extractor: CdfFabricExtractor, data_frame: DataFrame):
    # Processes data point subscription batches
    test_extractor.write_time_series_to_cdf(data_frame)


def start_replicator():
    # Start the replicator service
    pass

def stop_replicator():
    # Stop the replicator service
    pass

def setup_data_model_sync():
    # Setup data model sync service between CDF and Fabric
    pass