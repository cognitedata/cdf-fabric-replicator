import os
import pytest
import time
import pandas as pd
from unittest.mock import Mock
from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.metrics import safe_get
from cdf_fabric_replicator.metrics import Metrics
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
    assert_file_in_cdf,
    remove_file_from_cdf,
)
from tests.integration.integration_steps.fabric_steps import (
    write_timeseries_data_to_fabric,
    remove_time_series_data_from_fabric,
    prepare_test_dataframe_for_comparison,
    upload_file_to_lakehouse,
    remove_file_from_lakehouse,
)

TEST_FILE_NAME = "test_file.csv"
TEST_FILE_DIRECTORY = "tests/integration/resources/"
CDF_RETRIES = 5


@pytest.fixture(scope="session")
def test_extractor(request):
    stop_event = CancellationToken()
    extractor = CdfFabricExtractor(
        metrics=safe_get(Metrics), stop_event=stop_event, name="conftest"
    )
    extractor._initial_load_config(override_path=os.environ["TEST_CONFIG_PATH"])
    extractor.client = extractor.config.cognite.get_cognite_client(extractor.name)
    extractor.cognite_client = extractor.config.cognite.get_cognite_client(
        extractor.name
    )
    extractor.config.source.read_batch_size = request.param
    extractor._load_state_store()
    extractor.logger = Mock()
    extractor.data_set_id = None
    yield extractor


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


@pytest.fixture(scope="function")
def test_csv_file_path(test_extractor, azure_credential, cognite_client):
    os.makedirs(TEST_FILE_DIRECTORY, exist_ok=True)

    file_path = os.path.join(TEST_FILE_DIRECTORY, TEST_FILE_NAME)
    with open(file_path, "w") as file:
        file.write("Test data")
    yield file_path
    try:
        os.remove(file_path)
    except FileNotFoundError:
        pass
    remove_file_from_lakehouse(
        TEST_FILE_NAME,
        test_extractor.config.source.abfss_prefix,
        test_extractor.config.source.file_path,
        azure_credential,
    )
    remove_file_from_cdf(
        cognite_client,
        TEST_FILE_NAME,
        test_extractor.config.source.abfss_prefix,
        test_extractor.config.source.file_path,
    )


# Test for Timeseries Extractor service between CDF and Fabric
@pytest.mark.parametrize(
    "raw_time_series",
    [TimeSeriesGeneratorArgs(["int_test_fabcd_hist:mtu:39tic1092.pv"], 100)],
    indirect=True,
)
@pytest.mark.parametrize(
    "test_extractor",
    [1000, 10],
    indirect=True,
)  # Batch size for the extractor - 1000 yields all records in one batch, 10 yields 10 batches
def test_extractor_timeseries_service(
    cognite_client, raw_time_series, test_extractor, test_config
):
    # Run replicator to pick up new timeseries data points in Lakehouse
    test_extractor.extract_time_series_data(
        test_extractor.config.source.abfss_prefix,
        test_extractor.config.source.raw_time_series_path,
    )

    # Sleep for 30 seconds to allow replicator to process the data
    time.sleep(10)

    # Assert timeseries data is populated CDF
    assert_time_series_in_cdf_by_id(
        raw_time_series["externalId"].unique(), cognite_client
    )

    for external_id, group in raw_time_series.groupby("externalId"):
        group = prepare_test_dataframe_for_comparison(group)
        assert_data_points_df_in_cdf(external_id, group, cognite_client)


@pytest.mark.parametrize(
    "test_extractor",
    [1000],
    indirect=True,
)  # Batch size for the extractor
def test_extractor_abfss_file_upload(
    cognite_client, test_csv_file_path, test_extractor, azure_credential
):
    # Upload the file to Fabric
    (
        upload_file_to_lakehouse(
            TEST_FILE_NAME,
            test_csv_file_path,
            test_extractor.config.source.abfss_prefix,
            test_extractor.config.source.file_path,
            azure_credential,
        ),
    )
    # Run extractor upload
    test_extractor.upload_files_from_abfss(
        test_extractor.config.source.abfss_prefix
        + f"/{test_extractor.config.source.file_path}/"
    )

    # Assert that the file is available in CDF
    assert assert_file_in_cdf(
        cognite_client,
        TEST_FILE_NAME,
        test_extractor.config.source.abfss_prefix,
        test_extractor.config.source.file_path,
        CDF_RETRIES,
    )
