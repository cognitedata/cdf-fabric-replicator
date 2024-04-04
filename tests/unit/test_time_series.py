import pytest
import datetime
import pandas as pd

from cdf_fabric_replicator.time_series import TimeSeriesReplicator
from cognite.client.data_classes.datapoints_subscriptions import (
    DatapointsUpdate,
    Datapoints,
)
from cognite.extractorutils.metrics import BaseMetrics
from cognite.extractorutils.base import CancellationToken


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
def input_data_null():
    yield []


class TestTimeSeriesReplicator():
    metrics = BaseMetrics(extractor_name = "test_ts_duplicator", extractor_version = "1.0.0")
    replicator = TimeSeriesReplicator(metrics=metrics, stop_event=CancellationToken())

    def test_convert_updates_to_pandasdf_when_not_null(self, input_data_not_null):
        pd_df = pd.DataFrame(data=[
            ["id1", pd.to_datetime(1631234567, unit='s', utc=True), 1.23],
            ["id1", pd.to_datetime(1631234568, unit='s', utc=True), 4.56]],
            columns=["externalId", "timestamp", "value"])

        df = self.replicator.convert_updates_to_pandasdf(input_data_not_null)

        pd.testing.assert_frame_equal(df, pd_df)

    def test_convert_updates_to_pandasdf_when_null(self, input_data_null):
        df = self.replicator.convert_updates_to_pandasdf(input_data_null)
        assert df is None
