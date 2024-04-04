
import os
from azure.identity import DefaultAzureCredential
from deltalake import DeltaTable
from dateutil import tz
import pandas as pd
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from deltalake.writer import write_deltalake

TIMESTAMP_COLUMN = "timestamp"
DATA_MODEL_TIMESTAMP_COLUMNS = ["lastUpdatedTime", "createdTime"]

def lakehouse_table_name(table_name:str):
    return os.environ["LAKEHOUSE_ABFSS_PREFIX"] + "/Tables/" + table_name

def get_ts_delta_table(credential: DefaultAzureCredential, lakehouse_timeseries_path: str) -> DeltaTable:
    token = credential.get_token("https://storage.azure.com/.default")
    return DeltaTable(lakehouse_timeseries_path,storage_options={"bearer_token": token.token, "use_fabric_endpoint": "true"},)

def read_deltalake_timeseries(timeseries_path:str, credential: DefaultAzureCredential):
    delta_table = get_ts_delta_table(credential, timeseries_path)
    df = delta_table.to_pandas()
    return df

def prepare_lakehouse_dataframe_for_comparison(dataframe: pd.DataFrame, external_id: str) -> pd.DataFrame:
    dataframe = dataframe.loc[dataframe["externalId"] == external_id]
    dataframe[TIMESTAMP_COLUMN] = pd.to_datetime(dataframe[TIMESTAMP_COLUMN])
    if dataframe[TIMESTAMP_COLUMN].dt.tz is None:
        local_tz = tz.tzlocal()
        dataframe[TIMESTAMP_COLUMN] = dataframe[TIMESTAMP_COLUMN].dt.tz_localize(local_tz)
    dataframe[TIMESTAMP_COLUMN] = dataframe[TIMESTAMP_COLUMN].dt.tz_convert('UTC')
    dataframe[TIMESTAMP_COLUMN] = dataframe[TIMESTAMP_COLUMN].dt.round('s') # round to seconds to avoid microsecond differences
    return dataframe

def write_timeseries_data_to_fabric(credential: DefaultAzureCredential, data_frame: DataFrame, table_path: str):
     print(table_path)
     token = credential.get_token("https://storage.azure.com/.default").token
     table_path =table_path


     write_deltalake(table_path, data_frame, mode="append", storage_options={"bearer_token": token, "use_fabric_endpoint": "true"})
     return None

def remove_time_series_data_from_fabric(credential: DefaultAzureCredential, table_path:str):
    token = credential.get_token("https://storage.azure.com/.default").token
    try:
        DeltaTable(
            table_uri= table_path,
            storage_options={"bearer_token": token, "use_fabric_endpoint": "true"}
        ).delete()
    except Exception as e:
        pass

def prepare_test_dataframe_for_comparison(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe[TIMESTAMP_COLUMN] = pd.to_datetime(dataframe[TIMESTAMP_COLUMN])
    dataframe[TIMESTAMP_COLUMN] = dataframe[TIMESTAMP_COLUMN].dt.round('s') # round to seconds to avoid microsecond differences
    return dataframe

def assert_timeseries_data_in_fabric(external_id: str, data_points: pd.DataFrame, timeseries_path: str, azure_credential: DefaultAzureCredential):
    data_points_from_lakehouse = read_deltalake_timeseries(timeseries_path, azure_credential)
    lakehouse_dataframe = prepare_lakehouse_dataframe_for_comparison(data_points_from_lakehouse, external_id)
    test_dataframe = prepare_test_dataframe_for_comparison(data_points)
    assert_frame_equal(test_dataframe, lakehouse_dataframe, check_dtype=False)

def assert_data_model_instances_in_fabric(instance_table_paths: list, instance_dataframes: dict[str, pd.DataFrame], azure_credential: DefaultAzureCredential):
    # Assert the data model is populated in a Fabric lakehouse
    for path in instance_table_paths:
        delta_table = get_ts_delta_table(azure_credential, path)
        lakehouse_dataframe = delta_table.to_pandas()
        lakehouse_dataframe = lakehouse_dataframe.drop(columns=DATA_MODEL_TIMESTAMP_COLUMNS)
        table_name = path.split("Tables/")[1]
        assert_frame_equal(instance_dataframes[table_name], lakehouse_dataframe, check_dtype=False)

def assert_data_model_instances_update(update_dataframe: tuple, azure_credential: DefaultAzureCredential):
    # Assert the data model changes including versions are propagated to a Fabric lakehouse
    path = lakehouse_table_name(update_dataframe[0])
    delta_table = get_ts_delta_table(azure_credential, path)
    lakehouse_dataframe = delta_table.to_pandas()
    lakehouse_dataframe = lakehouse_dataframe.drop(columns=DATA_MODEL_TIMESTAMP_COLUMNS)
    assert_frame_equal( update_dataframe[1],lakehouse_dataframe, check_dtype=False)