import pytest
import datetime
import pandas as pd

from cdf_fabric_replicator.time_series import TimeSeriesReplicator
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointsUpdate,
    Datapoints,
    DatapointSubscriptionBatch
)
from cdf_fabric_replicator.config import SubscriptionsConfig
from cognite.extractorutils.metrics import BaseMetrics
from cognite.extractorutils.base import CancellationToken
import pytest
from unittest import mock
from cdf_fabric_replicator.time_series import TimeSeriesReplicator, SubscriptionsConfig, DatapointsUpdate
from cognite.client import CogniteClient


@pytest.fixture
def mock_token():
    yield "test_access_token"


@pytest.fixture
def mock_get_token(mocker, mock_token):
    yield mocker.patch("cdf_fabric_replicator.time_series.TimeSeriesReplicator.get_token", return_value=mock_token)

@pytest.fixture
def input_data_not_null():
    yield [DatapointsUpdate(
            time_series="test_ts",
            upserts=Datapoints(
                external_id="id1",
                timestamp=[1631234567000, 1631234568000],
                value=[1.23, 4.56]
            ),
            deletes=[]
        )]

@pytest.fixture
def input_data_batch(input_data_not_null):
    yield DatapointSubscriptionBatch(input_data_not_null, None, False, "test_next_cursor")

@pytest.fixture
def input_data_null():
    yield []

@pytest.fixture
def mock_write_deltalake(mocker):
    yield mocker.patch("cdf_fabric_replicator.time_series.TimeSeriesReplicator.write_pd_to_deltalake", return_value=None)

@pytest.fixture
def subcription_config():
    yield SubscriptionsConfig(
        external_id="test_external_id",
        partitions=[0],
        lakehouse_abfss_path_dps="test_lakehouse_abfss_path_dps",
        lakehouse_abfss_path_ts="test_lakehouse_abfss_path_ts"
    )

@pytest.fixture
def mock_send_time_series_to_lakehouse():
    with mock.patch.object(TimeSeriesReplicator, 'send_time_series_to_lakehouse_table') as mock_method:
        yield mock_method


class TestTimeSeriesReplicator():
    metrics = BaseMetrics(extractor_name = "test_ts_duplicator", extractor_version = "1.0.0")
    replicator = TimeSeriesReplicator(metrics=metrics, stop_event=CancellationToken())

    def test_convert_updates_to_pandasdf_when_not_null(self, input_data_not_null):
        pd_df = pd.DataFrame(data=[
            ["id1", datetime.datetime.fromtimestamp(1631234567), 1.23],
            ["id1", datetime.datetime.fromtimestamp(1631234568), 4.56]],
            columns=["externalId", "timestamp", "value"])

        df = self.replicator.convert_updates_to_pandasdf(input_data_not_null)

        pd.testing.assert_frame_equal(df, pd_df)

    def test_convert_updates_to_pandasdf_when_null(self, input_data_null):
        df = self.replicator.convert_updates_to_pandasdf(input_data_null)
        assert df is None
