import pytest
from integration_steps.cdf_steps import push_data_to_cdf, create_data_model_in_cdf, update_data_model_in_cdf
from integration_steps.time_series_generation import TimeSeriesGeneratorArgs
from integration_steps.fabric_steps import assert_timeseries_data_in_fabric, assert_data_model_in_fabric, assert_data_model_update
from integration_steps.service_steps import run_replicator, run_data_model_sync


# Test for Timeseries data integration service
@pytest.mark.parametrize(
    "time_series",
    [
        TimeSeriesGeneratorArgs(["int_test_fabcd_hist:mtu:39tic1091.pv"], 10)
    ],
    indirect=True,
)
def test_timeseries_data_integration_service(cognite_client, test_replicator, lakehouse_timeseries_path, time_series, azure_credential):
    # Push data points to CDF
    pushed_data = push_data_to_cdf(time_series, cognite_client)
    # Run replicator for data point subscription between CDF and Fabric
    run_replicator(test_replicator)
    # Assert timeseries data is populated in a Fabric lakehouse
    for ts_external_id, data_points in pushed_data.items():
        assert_timeseries_data_in_fabric(ts_external_id, data_points, lakehouse_timeseries_path, azure_credential)

# Test for data model sync service
# @pytest.mark.skip("Skipping test", allow_module_level=True)
def test_data_model_sync_service_creation(test_model, edge_table_path, instance_table_paths, cognite_client):
    # Create a data model in CDF
    create_data_model_in_cdf()
    # Run data model sync service between CDF and Fabric
    run_data_model_sync()
    # Assert the data model is populated in a Fabric lakehouse
    assert_data_model_in_fabric()

# @pytest.mark.skip("Skipping test", allow_module_level=True)
def test_data_model_sync_service_update(test_model, edge_table_path, instance_table_paths, cognite_client):
    # Run data model sync service between CDF and Fabric
    run_data_model_sync()
    # Update a data model in CDF and run data model sync
    update_data_model_in_cdf()
    run_data_model_sync()
    # Assert the data model changes including versions and last updated timestamps are propagated to a Fabric lakehouse
    assert_data_model_update()
