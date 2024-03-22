from cdf_fabric_replicator.time_series import TimeSeriesReplicator

def run_replicator(test_replicator: TimeSeriesReplicator):
    # Processes data point subscription batches
    test_replicator.process_subscriptions()

def start_replicator():
    # Start the replicator service
    pass

def stop_replicator():
    # Stop the replicator service
    pass

def setup_data_model_sync():
    # Setup data model sync service between CDF and Fabric
    pass