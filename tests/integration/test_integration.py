import time
import pytest
from integration_steps.cdf_steps import (
    push_data_to_cdf,
    create_data_model_in_cdf,
    update_data_model_in_cdf,
    remove_time_series_data
)
from integration_steps.time_series_generation import TimeSeriesGeneratorArgs
from integration_steps.fabric_steps import (
    assert_timeseries_data_in_fabric,
    assert_data_model_in_fabric,
    assert_data_model_update,
)
from integration_steps.service_steps import start_replicator, stop_replicator, setup_data_model_sync


# Test for Timeseries data integration service
@pytest.mark.skip("Skipping test", allow_module_level=True)
@pytest.mark.parametrize(
    "time_series",
    [
        TimeSeriesGeneratorArgs(["int_test_fabcd_hist:mtu:39tic1091.pv"], 10)
    ],
    indirect=True,
)
def test_timeseries_data_integration_service(cognite_client, lakehouse_timeseries_path, time_series, azure_credential):
    remove_time_series_data(time_series, cognite_client)
    # Setup integration service for data point subscription between CDF and Fabric
    start_replicator()
    # Push data to CDF
    pushed_data = push_data_to_cdf(time_series, cognite_client)
    # Assert timeseries data is populated in a Fabric lakehouse
    for ts_external_id, data_points in pushed_data.items():
        assert_timeseries_data_in_fabric(ts_external_id, data_points, lakehouse_timeseries_path, azure_credential)

@pytest.mark.skip("Skipping test", allow_module_level=True)
def test_timeseries_data_integration_service_restart(
    cognite_client, lakehouse_timeseries_path, time_series, azure_credential
):
    # Setup time series integration service between CDF and Fabric
    start_replicator()
    # Stop and restart the service
    stop_replicator()
    data_points = push_data_to_cdf(time_series, cognite_client)
    time.sleep(60)
    start_replicator()
    # Assert CDF timeseries data from the stopped time period is populated in a Fabric lakehouse
    assert_timeseries_data_in_fabric(data_points, lakehouse_timeseries_path, azure_credential)

# Test for data model sync service
@pytest.mark.skip("Skipping test", allow_module_level=True)
def test_data_model_sync_service_creation():
    # Setup data model sync service between CDF and Fabric
    setup_data_model_sync()
    # Create a data model in CDF
    create_data_model_in_cdf()
    # Assert the data model is populated in a Fabric lakehouse
    assert_data_model_in_fabric()

@pytest.mark.skip("Skipping test", allow_module_level=True)
def test_data_model_sync_service_update():
    # Setup data model sync service between CDF and Fabric and Create Model
    setup_data_model_sync()
    create_data_model_in_cdf()
    # Update a data model in CDF
    update_data_model_in_cdf()
    # Assert the data model changes including versions and last updated timestamps are propagated to a Fabric lakehouse
    assert_data_model_update()
