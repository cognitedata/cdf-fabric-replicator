import pytest
import logging
import pandas as pd
from unittest.mock import patch, call, Mock
from cdf_fabric_replicator.time_series import TimeSeriesReplicator
from cdf_fabric_replicator.config import SubscriptionsConfig
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointsUpdate,
    Datapoints,
)
from cognite.client.data_classes import (
    TimeSeries,
    DataPointSubscriptionWrite,
    filters as flt,
)
from cognite.client.data_classes.time_series import TimeSeriesProperty
from cdf_fabric_replicator import subscription


@pytest.fixture(scope="function")
def test_timeseries_replicator():
    replicator = TimeSeriesReplicator(metrics=Mock(), stop_event=Mock())
    # These attributes must be mocked here as they don't exist until __enter__ is called in the base class
    replicator.cognite_client = Mock()
    replicator.state_store = Mock()
    replicator.config = Mock()
    replicator.logger = Mock()
    return replicator


@pytest.fixture
def mock_subscription():
    return SubscriptionsConfig(
        external_id="test1",
        partitions=[0, 1],
        lakehouse_abfss_path_dps="dps",
        lakehouse_abfss_path_ts="ts",
    )


@pytest.fixture
def mock_subscription_2():
    return SubscriptionsConfig(
        external_id="test2",
        partitions=[0],
        lakehouse_abfss_path_dps="dps",
        lakehouse_abfss_path_ts="ts",
    )


@pytest.fixture
def mock_datapoints_update():
    return DatapointsUpdate(
        time_series="test_ts",
        upserts=Datapoints(
            external_id="id1",
            timestamp=[1631234567000, 1631234568000],
            value=[1.23, 4.56],
        ),
        deletes=[],
    )


@pytest.fixture
def datapoints_dataframe():
    return pd.DataFrame(
        data=[
            ["id1", pd.to_datetime(1631234567, unit="s", utc=True), 1.23],
            ["id1", pd.to_datetime(1631234568, unit="s", utc=True), 4.56],
        ],
        columns=["externalId", "timestamp", "value"],
    )


@pytest.fixture
def timeseries_dataframe():
    return pd.DataFrame(
        data=[
            [
                "id1",
                "test_ts",
                "test_description",
                False,
                False,
                "test_unit",
                {"key": "value"},
                "test_asset",
            ]
        ],
        columns=[
            "externalId",
            "name",
            "description",
            "isString",
            "isStep",
            "unit",
            "metadata",
            "assetExternalId",
        ],
    )


@pytest.fixture
def input_data_null():
    yield []


class TestTimeSeriesReplicator:
    @patch(
        "cdf_fabric_replicator.time_series.TimeSeriesReplicator.process_subscriptions"
    )
    def test_run_no_subscriptions(
        self, mock_process_subscriptions, test_timeseries_replicator
    ):
        test_timeseries_replicator.config = Mock(subscriptions=[])
        test_timeseries_replicator.run()
        mock_process_subscriptions.assert_not_called()

    @patch(
        "cdf_fabric_replicator.time_series.sub.autocreate_subscription",
        return_value=None,
    )
    @patch(
        "cdf_fabric_replicator.time_series.TimeSeriesReplicator.process_subscriptions"
    )
    @patch("cdf_fabric_replicator.time_series.time.sleep")
    def test_run(
        self,
        mock_sleep,
        mock_process_subscriptions,
        mock_autocreate,
        mock_subscription,
        test_timeseries_replicator,
    ):
        # Set the stop event to False for the first iteration and True for the second iteration
        test_timeseries_replicator.stop_event.is_set.side_effect = [
            False,
            True,
        ]

        # Set the subscriptions and poll time in the config
        test_timeseries_replicator.config = Mock(
            subscriptions=[mock_subscription], extractor=Mock(poll_time=1)
        )

        # Call the run method
        test_timeseries_replicator.run()

        # Check that the state store is initialized
        test_timeseries_replicator.state_store.initialize.assert_called_once()

        # Check that autocreate_subscription is called with the correct arguments
        mock_autocreate.assert_called_with(
            [mock_subscription],
            test_timeseries_replicator.cognite_client,
            test_timeseries_replicator.name,
        )

        # Check that process_subscriptions is called
        mock_process_subscriptions.assert_called_once()

        # Check that sleep is called
        mock_sleep.assert_called_once()

        # Check that extraction_pipelines.runs.create is called
        test_timeseries_replicator.cognite_client.extraction_pipelines.runs.create.assert_called_once()

    @patch("cdf_fabric_replicator.time_series.ThreadPoolExecutor", autospec=True)
    @patch("cdf_fabric_replicator.time_series.TimeSeriesReplicator.process_partition")
    def test_process_subscriptions(
        self,
        mock_process_partition,
        mock_executor,
        mock_subscription,
        mock_subscription_2,
        test_timeseries_replicator,
    ):
        test_subscriptions = [
            mock_subscription,
            mock_subscription_2,
        ]
        test_timeseries_replicator.config.subscriptions = test_subscriptions

        # Call process_subscriptions
        test_timeseries_replicator.process_subscriptions()

        # Check that ThreadPoolExecutor was called once for each partition
        expected_calls = [
            call().__enter__().submit(mock_process_partition, sub, part)
            for sub in test_subscriptions
            for part in sub.partitions
        ]  # Call enter submit for each partition
        mock_executor.assert_has_calls(expected_calls, any_order=True)

    @patch("cdf_fabric_replicator.time_series.TimeSeriesReplicator.send_to_lakehouse")
    def test_process_partition_when_updates(
        self, mock_send_to_lakehouse, mock_subscription, test_timeseries_replicator
    ):
        # Mock the return value of state_store.get_state
        test_timeseries_replicator.state_store.get_state.return_value = [None, None]

        # Mock the return value of cognite_client.time_series.subscriptions.iterate_data
        mock_update_batch = Mock(has_next=True, cursor="test_cursor")
        test_timeseries_replicator.cognite_client.time_series.subscriptions.iterate_data.return_value = [
            mock_update_batch
        ]

        # Call process_partition
        result = test_timeseries_replicator.process_partition(mock_subscription, 0)

        # Check that send_to_lakehouse was called with the correct arguments
        mock_send_to_lakehouse.assert_called_once_with(
            subscription=mock_subscription,
            update_batch=mock_update_batch,
            state_id="test1_0",
            send_now=False,
        )

        # Check that the return value is correct
        assert result == "No new data"

    @patch("cdf_fabric_replicator.time_series.TimeSeriesReplicator.send_to_lakehouse")
    def test_process_partition_when_no_updates(
        self, mock_send_to_lakehouse, mock_subscription, test_timeseries_replicator
    ):
        # Mock the return value of state_store.get_state
        test_timeseries_replicator.state_store.get_state.return_value = [None, None]

        # Mock the return value of cognite_client.time_series.subscriptions.iterate_data
        mock_update_batch = Mock(has_next=False, cursor="test_cursor")
        test_timeseries_replicator.cognite_client.time_series.subscriptions.iterate_data.return_value = [
            mock_update_batch
        ]

        # Call process_partition
        result = test_timeseries_replicator.process_partition(mock_subscription, 0)

        # Check that send_to_lakehouse was called with the correct arguments
        mock_send_to_lakehouse.assert_called_once_with(
            subscription=mock_subscription,
            update_batch=mock_update_batch,
            state_id="test1_0",
            send_now=True,
        )

        # Check that the return value is correct
        assert result == "test1_0 no more data at test_cursor"

    @patch(
        "cdf_fabric_replicator.time_series.TimeSeriesReplicator.send_data_point_to_lakehouse_table"
    )
    def test_send_to_lakehouse_send_now(
        self, mock_send_dp_to_lakehouse, test_timeseries_replicator
    ):
        test_timeseries_replicator.config.extractor.ingest_batch_size = 3
        test_timeseries_replicator.update_queue = []

        # Create a mock update_batch
        mock_update_batch = Mock(updates=[1, 2, 3])

        # Call send_to_lakehouse with send_now=True
        test_timeseries_replicator.send_to_lakehouse(
            subscription="test1",
            update_batch=mock_update_batch,
            state_id="test1_0",
            send_now=True,
        )

        # Check that send_data_point_to_lakehouse_table was called with the correct arguments
        mock_send_dp_to_lakehouse.assert_called_once_with(
            subscription="test1", updates=[1, 2, 3]
        )

        # Check that state store was synchronized
        test_timeseries_replicator.state_store.set_state.assert_called_once_with(
            external_id="test1_0", high=mock_update_batch.cursor
        )
        test_timeseries_replicator.state_store.synchronize.assert_called_once()

        # Check that update_queue is empty
        assert test_timeseries_replicator.update_queue == []

    @patch(
        "cdf_fabric_replicator.time_series.TimeSeriesReplicator.send_data_point_to_lakehouse_table"
    )
    def test_send_to_lakehouse_send_now_false(
        self, mock_send_dp_to_lakehouse, test_timeseries_replicator
    ):
        test_timeseries_replicator.config.extractor.ingest_batch_size = 3
        test_timeseries_replicator.update_queue = []

        # Create a mock update_batch
        mock_update_batch = Mock(updates=[1, 2, 3])

        # Call send_to_lakehouse with send_now=False
        test_timeseries_replicator.send_to_lakehouse(
            subscription="test1",
            update_batch=mock_update_batch,
            state_id="test1_0",
            send_now=False,
        )

        # Check that send_data_point_to_lakehouse_table was not called
        mock_send_dp_to_lakehouse.assert_not_called()

        # Check that state store was not synchronized
        test_timeseries_replicator.state_store.set_state.assert_not_called()
        test_timeseries_replicator.state_store.synchronize.assert_not_called()

        # Check that update_queue contains the updates
        assert test_timeseries_replicator.update_queue == [1, 2, 3]

    @patch(
        "cdf_fabric_replicator.time_series.TimeSeriesReplicator.write_pd_to_deltalake"
    )
    @patch(
        "cdf_fabric_replicator.time_series.TimeSeriesReplicator.send_time_series_to_lakehouse_table"
    )
    def test_send_data_point_to_lakehouse_table(
        self,
        mock_send_ts_to_lakehouse,
        mock_write_pd_to_deltalake,
        mock_subscription,
        mock_datapoints_update,
        datapoints_dataframe,
        test_timeseries_replicator,
    ):
        # Create a mock subscription and updates
        updates = [mock_datapoints_update]

        # Call send_data_point_to_lakehouse_table
        test_timeseries_replicator.send_data_point_to_lakehouse_table(
            mock_subscription, updates
        )

        # Check that send_time_series_to_lakehouse_table was called with the correct arguments
        mock_send_ts_to_lakehouse.assert_called_once_with(mock_subscription, updates[0])

        # Check that write_pd_to_deltalake was called once
        assert mock_write_pd_to_deltalake.call_count == 1

        # Get the arguments that write_pd_to_deltalake was called with
        args, kwargs = mock_write_pd_to_deltalake.call_args

        # Check the non-DataFrame arguments
        assert args[0] == "dps"

        pd.testing.assert_frame_equal(args[1], datapoints_dataframe)

    @patch(
        "cdf_fabric_replicator.time_series.TimeSeriesReplicator.write_pd_to_deltalake"
    )
    def test_send_time_series_to_lakehouse_table(
        self,
        mock_write_pd_to_deltalake,
        mock_subscription,
        mock_datapoints_update,
        timeseries_dataframe,
        test_timeseries_replicator,
    ):
        test_timeseries_replicator.cognite_client.time_series.retrieve.return_value = (
            TimeSeries(
                external_id="id1",
                name="test_ts",
                description="test_description",
                is_string=False,
                is_step=False,
                unit="test_unit",
                metadata={"key": "value"},
                asset_id=123,
            )
        )
        test_timeseries_replicator.cognite_client.assets.retrieve.return_value = Mock(
            external_id="test_asset"
        )

        # Call send_time_series_to_lakehouse_table
        test_timeseries_replicator.send_time_series_to_lakehouse_table(
            mock_subscription, mock_datapoints_update
        )

        # Check that time_series.retrieve was called with the correct arguments
        test_timeseries_replicator.cognite_client.time_series.retrieve.assert_called_once_with(
            external_id="id1"
        )

        # Check that assets.retrieve was called with the correct arguments
        test_timeseries_replicator.cognite_client.assets.retrieve.assert_called_once_with(
            id=123
        )

        # Check that write_pd_to_deltalake was called with the correct arguments
        args, kwargs = mock_write_pd_to_deltalake.call_args

        assert args[0] == "ts"

        pd.testing.assert_frame_equal(args[1], timeseries_dataframe, check_dtype=False)

    @patch(
        "cdf_fabric_replicator.time_series.TimeSeriesReplicator.write_pd_to_deltalake"
    )
    @patch("cdf_fabric_replicator.time_series.logging")
    def test_send_time_series_to_lakehouse_table_error(
        self,
        mock_logger,
        mock_write_pd_to_deltalake,
        mock_subscription,
        test_timeseries_replicator,
    ):
        test_timeseries_replicator.cognite_client.time_series.retrieve.return_value = (
            Mock()
        )

        # Call send_time_series_to_lakehouse_table
        test_timeseries_replicator.send_time_series_to_lakehouse_table(
            mock_subscription, Mock()
        )

        # Check that time_series.retrieve was called with the correct arguments
        test_timeseries_replicator.cognite_client.time_series.retrieve.assert_called_once()

        # Check that assets.retrieve was not called
        test_timeseries_replicator.cognite_client.assets.retrieve.assert_not_called()

        # Check that write_pd_to_deltalake was not called
        mock_write_pd_to_deltalake.assert_not_called()

        # Check that an error message was logged
        mock_logger.error.assert_called_once()

    def test_convert_updates_to_pandasdf_when_not_null(
        self, mock_datapoints_update, datapoints_dataframe, test_timeseries_replicator
    ):
        df = test_timeseries_replicator.convert_updates_to_pandasdf(
            [mock_datapoints_update]
        )

        pd.testing.assert_frame_equal(df, datapoints_dataframe)

    def test_convert_updates_to_pandasdf_when_null(
        self, input_data_null, test_timeseries_replicator
    ):
        # Call the convert_updates_to_pandasdf method with input_data_null
        df = test_timeseries_replicator.convert_updates_to_pandasdf(input_data_null)
        # Assert that the result is None
        assert df is None

    @patch(
        "cdf_fabric_replicator.time_series.TimeSeriesReplicator.get_token",
        return_value="test_token",
    )
    @patch("cdf_fabric_replicator.time_series.write_deltalake")
    def test_write_pd_to_deltalake(
        self, mock_write_deltalake, mock_get_token, test_timeseries_replicator
    ):
        # Create a mock DataFrame
        df = pd.DataFrame()

        # Call write_pd_to_deltalake
        test_timeseries_replicator.write_pd_to_deltalake("test_table", df)

        # Check that get_token was called
        mock_get_token.assert_called_once()

        # Check that write_deltalake was called with the correct arguments
        mock_write_deltalake.assert_called_once_with(
            table_or_uri="test_table",
            data=df,
            mode="append",
            engine="rust",
            schema_mode="merge",
            storage_options={
                "bearer_token": "test_token",
                "use_fabric_endpoint": "true",
            },
        )

    @patch(
        "cdf_fabric_replicator.time_series.DefaultAzureCredential.get_token",
        return_value=Mock(token="test_token"),
    )
    def test_get_token(self, mock_default_azure_credential, test_timeseries_replicator):
        # Call the get_token method of test_timeseries_replicator
        token = test_timeseries_replicator.get_token()

        # Check that the returned token is equal to the token returned by mock_default_azure_credential
        assert token == mock_default_azure_credential.return_value.token

    def test_create_subscription(self, test_timeseries_replicator):
        num_partitions = 5
        external_id = "test_external_id"
        name = "test_name"

        with patch.object(
            test_timeseries_replicator.cognite_client.time_series.subscriptions,
            "create",
        ) as mock_create:
            subscription.create_subscription(
                test_timeseries_replicator.cognite_client,
                external_id,
                name,
                num_partitions,
                logging.getLogger("integration_tests"),
            )

            mock_create.assert_called_once_with(
                DataPointSubscriptionWrite(
                    external_id=external_id,
                    name=name,
                    partition_count=num_partitions,
                    filter=flt.Exists(TimeSeriesProperty.external_id),
                )
            )
