import os
import pytest
import time
import pandas as pd
from unittest.mock import Mock
from cognite.extractorutils.base import CancellationToken
from cdf_fabric_replicator.extractor import CdfFabricExtractor
from tests.integration.integration_steps.time_series_generation import (
    generate_raw_timeseries_set,
    generate_timeseries,
    TimeSeriesGeneratorArgs,
)
from tests.integration.integration_steps.cdf_steps import (
    delete_state_store_in_cdf,
    remove_time_series_data,
    push_time_series_to_cdf,
    assert_time_series_in_cdf_by_id,
    assert_data_points_df_in_cdf,
)
from tests.integration.integration_steps.service_steps import run_extractor
from tests.integration.integration_steps.fabric_steps import (
    write_timeseries_data_to_fabric,
    remove_time_series_data_from_fabric,
    prepare_test_dataframe_for_comparison,
)


@pytest.fixture(scope="session")
def test_extractor():
    stop_event = CancellationToken()
    extractor = CdfFabricExtractor(stop_event=stop_event, name="conftest")
    extractor._initial_load_config(override_path=os.environ["TEST_CONFIG_PATH"])
    extractor.client = extractor.config.cognite.get_cognite_client(extractor.name)
    extractor.cognite_client = extractor.config.cognite.get_cognite_client(
        extractor.name
    )
    extractor._load_state_store()
    extractor.logger = Mock()
    yield extractor
    try:
        os.remove("states.json")
    except FileNotFoundError:
        pass


@pytest.fixture(scope="function")
def raw_time_series(request, azure_credential, cognite_client, test_extractor):
    timeseries_set = generate_raw_timeseries_set(request.param)
    df = pd.DataFrame(timeseries_set, columns=["externalId", "timestamp", "value"])
    remove_time_series_data_from_fabric(
        azure_credential,
        test_extractor.config.source.abfss_prefix
        + "/"
        + test_extractor.config.source.raw_time_series_path,
    )
    write_timeseries_data_to_fabric(
        azure_credential,
        df,
        test_extractor.config.source.abfss_prefix
        + "/"
        + test_extractor.config.source.raw_time_series_path,
    )

    unique_external_ids = set()
    generated_timeseries = []
    for ts in timeseries_set:
        if ts["externalId"] not in unique_external_ids:
            push_time_series_to_cdf(
                [generate_timeseries(ts["externalId"], 1)], cognite_client
            )
            generated_timeseries.append(generate_timeseries(ts["externalId"], 1))
            unique_external_ids.add(ts["externalId"])
    yield df
    for ts in generated_timeseries:
        remove_time_series_data(generated_timeseries, cognite_client)

    remove_time_series_data_from_fabric(
        azure_credential,
        test_extractor.config.source.abfss_prefix
        + "/"
        + test_extractor.config.source.raw_time_series_path,
    )

    delete_state_store_in_cdf(
        test_extractor.config.subscriptions,
        test_extractor.config.extractor.state_store.raw.database,
        test_extractor.config.extractor.state_store.raw.table,
        cognite_client,
    )


# Test for Timeseries Extractor service between CDF and Fabric
@pytest.mark.parametrize(
    "raw_time_series",
    [TimeSeriesGeneratorArgs(["int_test_fabcd_hist:mtu:39tic1092.pv"], 10)],
    indirect=True,
)
def test_extractor_timeseries_service(cognite_client, raw_time_series, test_extractor):
    # Run replicator to pick up new timeseries data points in Lakehouse
    run_extractor(test_extractor, raw_time_series)

    # Sleep for 30 seconds to allow replicator to process the data
    time.sleep(10)

    # Assert timeseries data is populated CDF
    assert_time_series_in_cdf_by_id(
        raw_time_series["externalId"].unique(), cognite_client
    )

    for external_id, group in raw_time_series.groupby("externalId"):
        group = prepare_test_dataframe_for_comparison(group)
        assert_data_points_df_in_cdf(external_id, group, cognite_client)
