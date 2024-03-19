
from azure.identity import DefaultAzureCredential
from deltalake import DeltaTable
from pandas.testing import assert_frame_equal

def get_ts_delta_table(credential: DefaultAzureCredential, lakehouse_timeseries_path: str):
    token = credential.get_token("https://storage.azure.com/.default")
    return DeltaTable(lakehouse_timeseries_path,storage_options={"bearer_token": token.token, "use_fabric_endpoint": "true"},)

def read_deltalake_timeseries(timeseries_path:str, credential: DefaultAzureCredential):
    delta_table = get_ts_delta_table(credential, timeseries_path)
    df = delta_table.to_pandas()
    return df

def assert_timeseries_data_in_fabric(external_id, data_points, timeseries_path, azure_credential: DefaultAzureCredential):
    data_points_from_lakehouse = read_deltalake_timeseries(timeseries_path, azure_credential)
    filtered_dataframe = data_points_from_lakehouse.loc[data_points_from_lakehouse["externalId"] == external_id]
    assert_frame_equal(data_points, filtered_dataframe, check_dtype=False)

def assert_data_model_in_fabric():
    # Assert the data model is populated in a Fabric lakehouse
    pass

def assert_data_model_update():
    # Assert the data model changes including versions and last updated timestamps are propagated to a Fabric lakehouse
    pass