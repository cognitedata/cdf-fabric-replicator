import logging

from urllib.parse import urlparse

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import (
    DataLakeFileClient,
    DataLakeServiceClient,
)
from cognite.client.data_classes import (
    EventWrite,
    ExtractionPipelineRunWrite,
    FileMetadata,
    TimeSeriesWrite,
)
from cognite.client.exceptions import CogniteNotFoundError, CogniteAPIError

from cognite.extractorutils import Extractor
from cognite.extractorutils.base import CancellationToken
from deltalake import DeltaTable
from pandas import DataFrame
from typing import Any, Literal, Generator
import pandas as pd

from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config
from cdf_fabric_replicator.metrics import Metrics


class CdfFabricExtractor(Extractor[Config]):
    def __init__(
        self,
        metrics: Metrics,
        stop_event: CancellationToken,
        name: str = "cdf_fabric_extractor",
    ) -> None:
        super().__init__(
            name=name,
            description="CDF Fabric Extractor",
            config_class=Config,
            metrics=metrics,
            version=__version__,
        )
        self.metrics = metrics
        self.azure_credential = DefaultAzureCredential()
        self.stop_event = stop_event
        self.logger = logging.getLogger(self.name)

    def run(self) -> None:
        self.config = self.get_current_config()
        self.client = self.config.cognite.get_cognite_client("cdf-fabric-extractor")
        self.state_store = self.get_current_statestore()
        self.state_store.initialize()

        if not self.config.source:
            self.logger.error("No source path or directory provided")
            return

        self.data_set_id = (
            int(self.config.source.data_set_id)
            if self.config.source.data_set_id
            else None
        )

        self.logger.debug(f"Current Extractor Config: {self.config.extractor}")

        while not self.stop_event.is_set():
            token = self.azure_credential.get_token(
                "https://storage.azure.com/.default"
            ).token

            self.run_extraction_pipeline(status="seen")

            if (
                self.config.source.raw_time_series_path
                and self.config.destination.time_series_prefix
            ):
                self.extract_time_series_data(
                    self.config.source.abfss_prefix,
                    self.config.source.raw_time_series_path,
                )

            if self.config.source.event_path:
                state_id = f"{self.config.source.event_path}-state"
                self.write_event_data_to_cdf(
                    self.config.source.abfss_prefix
                    + "/"
                    + self.config.source.event_path,
                    token=token,
                    state_id=state_id,
                )

            if self.config.source.file_path:
                self.upload_files_from_abfss(
                    self.config.source.abfss_prefix + "/" + self.config.source.file_path
                )

            if self.config.source.raw_tables:
                for path in self.config.source.raw_tables:
                    state_id = f"{path}-state"
                    self.write_raw_tables_to_cdf(
                        self.config.source.abfss_prefix + "/" + path.raw_path,
                        token=token,
                        state_id=state_id,
                        table_name=path.table_name,
                        db_name=path.db_name,
                    )

            self.logger.debug("Sleep for 5 seconds")
            self.stop_event.wait(5)

        self.logger.info("Stop event set. Exiting...")

    def get_asset_ids(self, asset_external_ids: list) -> list:
        asset_ids = []
        for external_id in asset_external_ids:
            try:
                self.logger.debug(f"Retrieving asset with external id {external_id}")
                asset = self.client.assets.retrieve(external_id=external_id)
                if asset:
                    asset_ids.append(asset.id)
            except CogniteNotFoundError as e:
                self.logger.error(f"Asset with external id {external_id} not found")
                raise e
            except CogniteAPIError as e:
                self.logger.error(f"Error while retrieving asset: {e}")
                raise e

        return asset_ids

    def parse_abfss_url(self, url: str) -> tuple[str, str, str]:
        parsed_url = urlparse(url)

        container_id = parsed_url.netloc.split("@")[0]
        account_name = parsed_url.netloc.split("@")[1].split(".")[0]
        file_path = parsed_url.path

        return container_id, account_name, file_path

    def get_service_client_token_credential(
        self, account_name: str
    ) -> DataLakeServiceClient:
        account_url = f"https://{account_name}.dfs.fabric.microsoft.com"
        token_credential = DefaultAzureCredential()
        service_client = DataLakeServiceClient(account_url, credential=token_credential)

        return service_client

    def upload_files_from_abfss(self, file_path: str) -> None:
        container_name, account_name, file_path = self.parse_abfss_url(file_path)

        service_client = self.get_service_client_token_credential(account_name)

        file_system_client = service_client.get_file_system_client(container_name)

        for file in file_system_client.get_paths(file_path):
            if not file.is_directory:
                file_client = file_system_client.get_file_client(file.name)

                state = self.state_store.get_state(file.name)
                if state and state[0] != file.last_modified.timestamp():
                    res = self.upload_files_to_cdf(file_client, file)
                    self.logger.info(
                        f"Uploaded file {file.name} to CDF with id {res.id}"
                    )
                    self.run_extraction_pipeline(status="success")
                    self.state_store.set_state(
                        file.name, file.last_modified.timestamp()
                    )
                    self.state_store.synchronize()

    def upload_files_to_cdf(
        self, file_client: DataLakeFileClient, file: Any
    ) -> FileMetadata:
        content = file_client.download_file().readall()
        file_name = file.name.split("/")[-1]

        created_time = int(file.creation_time.timestamp() * 1000)
        modified_time = int(file.last_modified.timestamp() * 1000)
        try:
            return self.cognite_client.files.upload_bytes(
                content=content,
                name=file_name,
                external_id=file.name,
                data_set_id=self.data_set_id,
                source_created_time=created_time,
                source_modified_time=modified_time,
                overwrite=True,
            )
        except CogniteAPIError as e:
            self.logger.error(f"Error while uploading file to CDF: {e}")
            raise e

    def extract_time_series_data(
        self, abfss_prefix: str, raw_time_series_path: str
    ) -> None:
        token = self.azure_credential.get_token(
            "https://storage.azure.com/.default"
        ).token
        for time_series_data in self.convert_lakehouse_data_to_df_batch(
            abfss_prefix + "/" + raw_time_series_path,
            token=token,
        ):
            self.write_time_series_to_cdf(time_series_data)

    def write_time_series_to_cdf(self, data_frame: DataFrame) -> None:
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
                latest_process_time = df_to_be_written["timestamp"].max()

                df_to_be_written.loc[:, "externalId"] = df_to_be_written.apply(
                    lambda row: self.config.destination.time_series_prefix
                    + row["externalId"],
                    axis=1,
                )
                df_to_be_written = df_to_be_written.pivot(
                    index="timestamp", columns="externalId", values="value"
                )
                df_to_be_written.index = pd.to_datetime(df_to_be_written.index)
                try:
                    self.client.time_series.data.insert_dataframe(df_to_be_written)
                except CogniteNotFoundError as notFound:
                    missing_xids = notFound.not_found
                    for new_timeserie in missing_xids:
                        # Create the missing time series
                        self.client.time_series.create(
                            TimeSeriesWrite(
                                name=new_timeserie["externalId"],
                                external_id=new_timeserie["externalId"],
                                is_string=True,
                                data_set_id=self.data_set_id,
                            )
                        )
                except CogniteAPIError as e:
                    self.logger.error(
                        f"Error while writing time series data to CDF: {e}"
                    )
                    raise e

                self.set_state(state_id, str(latest_process_time))

    def write_event_data_to_cdf(
        self, file_path: str, token: str, state_id: str
    ) -> None:
        df = self.convert_lakehouse_data_to_df(file_path, token)

        if str(self.state_store.get_state(state_id)[0]) != str(len(df)):
            events = self.get_events(df)

            try:
                self.client.events.upsert(events)
            except CogniteAPIError as e:
                self.logger.error(f"Error while writing event data to CDF: {e}")
                raise e

            self.run_extraction_pipeline(status="success")

            self.set_state(state_id, str(len(df)))

    def write_raw_tables_to_cdf(
        self, file_path: str, token: str, state_id: str, table_name: str, db_name: str
    ) -> None:
        df = self.convert_lakehouse_data_to_df(file_path, token)

        if str(self.state_store.get_state(state_id)[0]) != str(len(df)):
            try:
                self.client.raw.rows.insert_dataframe(
                    db_name=db_name,
                    table_name=table_name,
                    dataframe=df,
                    ensure_parent=True,
                )
            except CogniteAPIError as e:
                self.logger.error(f"Error while writing raw data to CDF: {e}")
                self.run_extraction_pipeline(status="failure")

                raise e

            self.run_extraction_pipeline(status="success")
            self.set_state(state_id, str(len(df)))
        else:
            self.run_extraction_pipeline(status="seen")

    def set_state(self, state_id: str, value: str) -> None:
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
                data_set_id=self.data_set_id,
            )

            events.append(new_event)
        return events

    def convert_lakehouse_data_to_df(self, file_path: str, token: str) -> DataFrame:
        try:
            dt = DeltaTable(
                file_path,
                storage_options={"bearer_token": token, "user_fabric_endpoint": "true"},
            )
            return dt.to_pandas()
        except Exception as e:
            self.logger.error(
                f"Error while converting lakehouse data to DataFrame: {e}"
            )
            raise e

    def convert_lakehouse_data_to_df_batch(
        self, file_path: str, token: str
    ) -> Generator[DataFrame, None, None]:
        try:
            dt = DeltaTable(
                file_path,
                storage_options={"bearer_token": token, "user_fabric_endpoint": "true"},
            )
            dataset = dt.to_pyarrow_dataset()
            batch_set = dataset.to_batches(
                batch_size=self.config.source.read_batch_size
            )
            for batch in batch_set:
                yield batch.to_pandas()
        except Exception as e:
            self.logger.error(
                f"Error while converting lakehouse data to DataFrame: {e}"
            )
            raise e

    def run_extraction_pipeline(
        self, status: Literal["success", "failure", "seen"]
    ) -> None:
        if self.config.cognite.extraction_pipeline:
            try:
                self.cognite_client.extraction_pipelines.runs.create(
                    ExtractionPipelineRunWrite(
                        status=status,
                        extpipe_external_id=str(
                            self.config.cognite.extraction_pipeline.external_id
                        ),
                    )
                )
            except CogniteAPIError as e:
                self.logger.error(f"Error while running extraction pipeline: {e}")
                raise e
