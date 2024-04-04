from cdf_fabric_replicator.time_series import TimeSeriesReplicator
from cdf_fabric_replicator.extractor import CdfFabricExtractor
from pandas import DataFrame
from cdf_fabric_replicator.data_modeling import DataModelingReplicator

def run_replicator(test_replicator: TimeSeriesReplicator):
    # Processes data point subscription batches
    test_replicator.process_subscriptions()


def run_extractor(test_extractor: CdfFabricExtractor, data_frame: DataFrame):
    # Processes data point subscription batches
    test_extractor.write_time_series_to_cdf(data_frame)


def run_data_modeling_replicator(test_data_modeling_replicator: DataModelingReplicator):
    # Processes spaces for data-modeling changes
    test_data_modeling_replicator.process_spaces()

def start_replicator():
    # Start the replicator service
    pass

def stop_replicator():
    # Stop the replicator service
    pass

def run_data_model_sync():
    # Run data model sync service between CDF and Fabric
    pass