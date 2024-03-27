import pytest
from integration_steps.cdf_steps import assert_state_store_in_cdf, push_data_to_cdf
from integration_steps.time_series_generation import TimeSeriesGeneratorArgs
from integration_steps.fabric_steps import assert_timeseries_data_in_fabric
from integration_steps.service_steps import run_replicator


# Test for Timeseries data integration service between CDF and Fabric
@pytest.mark.parametrize(
    "time_series",
    [
        TimeSeriesGeneratorArgs(["int_test_fabcd_hist:mtu:39tic1091.pv"], 10)
    ],
    indirect=True,
)
def test_timeseries_data_integration_service(cognite_client, test_replicator, lakehouse_timeseries_path, time_series, azure_credential, remote_state_store):
    # Push data points to CDF
    pushed_data = push_data_to_cdf(time_series, cognite_client)
    # Run replicator for data point subscription between CDF and Fabric
    run_replicator(test_replicator)
    # Assert timeseries data is populated in a Fabric lakehouse
    for ts_external_id, data_points in pushed_data.items():
        assert_timeseries_data_in_fabric(ts_external_id, data_points, lakehouse_timeseries_path, azure_credential)
    # Assert state store is populated in CDF
    assert_state_store_in_cdf(
        test_replicator.config.subscriptions, 
        test_replicator.config.extractor.state_store.raw.database, 
        test_replicator.config.extractor.state_store.raw.table, 
        cognite_client)

