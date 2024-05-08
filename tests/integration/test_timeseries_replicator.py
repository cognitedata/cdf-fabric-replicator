import os
import logging
import pytest
from time import sleep
from unittest.mock import Mock
from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.metrics import safe_get
from cdf_fabric_replicator.metrics import Metrics
from cdf_fabric_replicator.time_series import TimeSeriesReplicator
from tests.integration.integration_steps.time_series_generation import (
    generate_timeseries_set,
    TimeSeriesGeneratorArgs,
)
from tests.integration.integration_steps.cdf_steps import (
    delete_state_store_in_cdf,
    remove_time_series_data,
    push_time_series_to_cdf,
    push_data_to_cdf,
    remove_subscriptions,
    assert_state_store_in_cdf,
    assert_subscription_created_in_cdf,
)
from tests.integration.integration_steps.service_steps import run_replicator
from tests.integration.integration_steps.fabric_steps import (
    delete_delta_table_data,
    assert_timeseries_data_in_fabric,
)
from cdf_fabric_replicator import subscription


@pytest.fixture(scope="function")
def test_replicator():
    stop_event = CancellationToken()
    replicator = TimeSeriesReplicator(metrics=safe_get(Metrics), stop_event=stop_event)
    replicator._initial_load_config(override_path=os.environ["TEST_CONFIG_PATH"])
    replicator.cognite_client = replicator.config.cognite.get_cognite_client(
        replicator.name
    )
    replicator.logger = Mock()
    yield replicator
    try:
        os.remove("states.json")
    except FileNotFoundError:
        pass


@pytest.fixture()
def remote_state_store(cognite_client, test_replicator):
    test_replicator._load_state_store()
    state_store = test_replicator.state_store
    yield state_store
    delete_state_store_in_cdf(
        test_replicator.config.subscriptions,
        test_replicator.config.extractor.state_store.raw.database,
        test_replicator.config.extractor.state_store.raw.table,
        cognite_client,
    )


@pytest.fixture()
def time_series(request, cognite_client):
    sub_name = "testSubscription"
    timeseries_set = generate_timeseries_set(request.param)
    remove_time_series_data(timeseries_set, cognite_client)
    remove_subscriptions(sub_name, cognite_client)
    push_time_series_to_cdf(timeseries_set, cognite_client)
    sleep(5)
    yield timeseries_set
    remove_time_series_data(timeseries_set, cognite_client)
    remove_subscriptions(sub_name, cognite_client)


@pytest.fixture(scope="session")
def lakehouse_timeseries_path(azure_credential):
    lakehouse_timeseries_path = (
        os.environ["LAKEHOUSE_ABFSS_PREFIX"] + "/Tables/" + os.environ["DPS_TABLE_NAME"]
    )
    delete_delta_table_data(azure_credential, lakehouse_timeseries_path)
    yield lakehouse_timeseries_path
    delete_delta_table_data(azure_credential, lakehouse_timeseries_path)


# Test for Timeseries data integration service between CDF and Fabric
@pytest.mark.parametrize(
    "time_series",
    [TimeSeriesGeneratorArgs(["int_test_fabcd_hist:mtu:39tic1091.pv"], 10)],
    indirect=True,
)
def test_timeseries_data_integration_service(
    cognite_client,
    test_replicator,
    lakehouse_timeseries_path,
    time_series,
    azure_credential,
    remote_state_store,
):
    # Autocreate subscription in CDF
    subscription.autocreate_subscription(
        test_replicator.config.subscriptions,
        cognite_client,
        "TestSubscription",
        logging.getLogger("integration_tests"),
    )
    # Assert subscription created in CDF
    assert_subscription_created_in_cdf(
        test_replicator.config.subscriptions[0],
        cognite_client,
    )
    # Push data points to CDF
    pushed_data = push_data_to_cdf(time_series, cognite_client)
    # Run replicator for data point subscription between CDF and Fabric
    run_replicator(test_replicator)
    # Assert timeseries data is populated in a Fabric lakehouse
    for ts_external_id, data_points in pushed_data.items():
        assert_timeseries_data_in_fabric(
            ts_external_id, data_points, lakehouse_timeseries_path, azure_credential
        )
    # Assert state store is populated in CDF
    assert_state_store_in_cdf(
        test_replicator.config.subscriptions,
        remote_state_store.database,
        remote_state_store.table,
        cognite_client,
    )
