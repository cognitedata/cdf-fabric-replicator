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
