import time
import logging

from urllib.parse import urlparse

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import (
    DataLakeServiceClient,
)
from cognite.client.data_classes import EventWrite, ExtractionPipelineRunWrite, FileMetadata
from cognite.extractorutils import Extractor
from cognite.extractorutils.base import CancellationToken
from deltalake import DeltaTable
from pandas import DataFrame
import pandas as pd

from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config
# from cdf_fabric_extractor.config import Config


class CdfFabricExtractor(Extractor[Config]):
    def __init__(self, stop_event: CancellationToken) -> None:
        super().__init__(
            name="cdf_fabric_extractor",
            description="CDF Fabric Extractor",
            config_class=Config,
            version=__version__,
        )
        self.azure_credential = DefaultAzureCredential()
        self.stop_event = stop_event

    def run(self) -> None:
        self.config = self.get_current_config()
        self.client = self.config.cognite.get_cognite_client("cdf-fabric-extractor")
        self.state_store = self.get_current_statestore()
        self.state_store.initialize()

        if not self.config.source:
            self.logger.error("No source path or directory provided")
            return

        token = self.azure_credential.get_token("https://storage.azure.com/.default").token
        
        while self.stop_event.is_set() is False:
            self.run_extraction_pipeline(status="seen")

            if self.config.source.raw_time_series_path and self.config.destination.time_series_prefix:
                time_series_data = self.convert_lakehouse_data_to_df(
                    self.config.source.raw_time_series_path, token=token
                )
                self.write_time_series_to_cdf(time_series_data)

            if self.config.source.event_path:
                state_id = f"{self.config.source.event_path}-state"
                self.write_event_data_to_cdf(self.config.source.event_path, token=token, state_id=state_id)

            if self.config.source.abfss_directory:
                self.download_files_from_abfss(self.config.source.abfss_directory)

            time.sleep(5)

    def get_asset_ids(self, asset_external_ids: list) -> list:
        asset_ids = []
        for external_id in asset_external_ids:
            asset = self.client.assets.retrieve(external_id=external_id)
            if asset:
                asset_ids.append(asset.id)

        return asset_ids

    def parse_abfss_url(self, url: str) -> tuple[str, str, str]:
        parsed_url = urlparse(url)

        container_id = parsed_url.netloc.split("@")[0]
        account_name = parsed_url.netloc.split("@")[1].split(".")[0]
        file_path = parsed_url.path

        return container_id, account_name, file_path

    def get_service_client_token_credential(self, account_name: str) -> DataLakeServiceClient:
        account_url = f"https://{account_name}.dfs.fabric.microsoft.com"
        token_credential = DefaultAzureCredential()
        service_client = DataLakeServiceClient(account_url, credential=token_credential)

        return service_client

    def download_files_from_abfss(self, abfss_directory: str) -> None:
        container_name, account_name, file_path = self.parse_abfss_url(abfss_directory)

        service_client = self.get_service_client_token_credential(account_name)

        file_system_client = service_client.get_file_system_client(container_name)

        for file in file_system_client.get_paths(file_path):
            if not file.is_directory:
                file_client = file_system_client.get_file_client(file.name)

                state = self.state_store.get_state(file.name)
                if state and state[0] == file.last_modified.timestamp():
                    continue

                res = self.upload_files_to_cdf(file_client, file)
                self.logger.info(f"Uploaded file {file.name} to CDF with id {res.id}")
                self.run_extraction_pipeline(status = "success")
                self.state_store.set_state(file.name, file.last_modified.timestamp())
                self.state_store.synchronize()

    def upload_files_to_cdf(self, file_client:  DataLakeServiceClient, file) -> FileMetadata:
        content = file_client.download_file().readall()
        file_name = file.name.split("/")[-1]
        data_set_id = self.config.source.data_set_id if self.config.source.data_set_id else None
        created_time = int(file.creation_time.timestamp() * 1000)
        modified_time = int(file.last_modified.timestamp() * 1000)
        return self.cognite_client.files.upload_bytes(
                        content=content, 
                        name=file_name, 
                        external_id=file.name, 
                        data_set_id=data_set_id, 
                        source_created_time=created_time, 
                        source_modified_time=modified_time, 
                        overwrite=True)

    def write_time_series_to_cdf(self, data_frame:DataFrame ) -> None:
        external_ids = data_frame["externalId"].unique()
        for external_id in external_ids:
            df = data_frame[data_frame["externalId"] == external_id]
            state_id = f"{self.config.source.raw_time_series_path}-{external_id}-state"
            df_to_be_written = None
            if self.state_store.get_state(state_id)[0] is None:
                df_to_be_written = df
            else:
                latest_written_time = self.state_store.get_state(state_id)[0]
                df_to_be_written = df[df["timestamp"] > latest_written_time]

            if len(df_to_be_written) > 0:                
                latest_process_time = df_to_be_written['timestamp'].max()

                df_to_be_written.loc[:, "externalId"] = df_to_be_written.apply(
                    lambda row: self.config.destination.time_series_prefix + row["externalId"], axis=1
                )
                df_to_be_written = df_to_be_written.pivot(index="timestamp", columns="externalId", values="value")
                df_to_be_written.index = pd.to_datetime(df_to_be_written.index)
                self.client.time_series.data.insert_dataframe(df_to_be_written)

                self.set_state(state_id, latest_process_time)

    def write_event_data_to_cdf(self, file_path: str, token: str, state_id: str) -> None:
        df = self.convert_lakehouse_data_to_df(file_path, token)

        if str(self.state_store.get_state(state_id)[0]) != str(len(df)):
            events = self.get_events(df)

            self.client.events.upsert(events)
            self.run_extraction_pipeline(status="success")

            self.set_state(state_id, str(len(df)))

    def set_state(self, state_id: str, value) -> None:
        self.state_store.set_state(state_id, value)
        self.state_store.synchronize()

    def get_events(self, df: DataFrame) -> list[EventWrite]:
        events = []
        for row in df.iterrows():
            new_event = EventWrite(
                external_id=row[1]["externalId"],
                start_time=row[1]["startTime"],
                end_time=row[1]["endTime"],
                type=row[1]["type"],
                subtype=row[1]["subtype"],
                metadata=row[1]["metadata"],
                description=row[1]["description"],
                asset_ids=self.get_asset_ids(row[1]["assetExternalIds"]),
                data_set_id=self.config.source.data_set_id if self.config.source.data_set_id else None,
            )

            events.append(new_event)
        return events

    def convert_lakehouse_data_to_df(self, file_path: str, token: str) -> DataFrame:
        dt = DeltaTable(
                    file_path,
                    storage_options={"bearer_token": token, "user_fabric_endpoint": "true"},
                )
        return dt.to_pandas()

    def run_extraction_pipeline(self, status: str) -> None:
        if self.config.cognite.extraction_pipeline:
            self.cognite_client.extraction_pipelines.runs.create(
                ExtractionPipelineRunWrite(
                    status=status, 
                    extpipe_external_id=str(self.config.cognite.extraction_pipeline.external_id)
                )
            )
