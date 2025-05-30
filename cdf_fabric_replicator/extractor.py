import logging
from datetime import datetime, timezone
from hashlib import md5
from typing import Any, Generator, Iterable, List, Literal, Optional
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as ds
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import (
    DataLakeFileClient,
    DataLakeServiceClient,
)
from deltalake import DeltaTable
from pandas import DataFrame

from cdf_fabric_replicator import __version__ as fabric_replicator_version
from cdf_fabric_replicator.config import Config
from cdf_fabric_replicator.metrics import Metrics
from cognite.client.data_classes import (
    EventWrite,
    ExtractionPipelineRunWrite,
    FileMetadata,
    TimeSeriesWrite,
)
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.extractorutils import Extractor
from cognite.extractorutils.base import CancellationToken


class CdfFabricExtractor(Extractor[Config]):
    def __init__(
        self,
        metrics: Metrics,
        stop_event: CancellationToken,
        name: str = "cdf_fabric_extractor",
        override_config_path: Optional[str] = None,
    ) -> None:
        super().__init__(
            name=name,
            description="CDF Fabric Extractor",
            config_class=Config,
            metrics=metrics,
            version=fabric_replicator_version,
            config_file_path=override_config_path,
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
                    self.config.source.abfss_prefix
                    + "/"
                    + self.config.source.raw_time_series_path,
                )

            if self.config.source.event_path:
                state_id = f"{self.config.source.event_path}-state"
                self.write_event_data_to_cdf(
                    self.config.source.abfss_prefix
                    + "/"
                    + self.config.source.event_path,
                    token=token,
                    state_id=state_id,
                    incremental_field=self.config.source.event_path_incremental_field,
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
                        key_fields=path.key_fields,
                        md5_key=path.md5_key,
                        incremental_field=path.incremental_field,
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

        files_uploaded = 0
        for file in file_system_client.get_paths(file_path):
            if not file.is_directory:
                file_client = file_system_client.get_file_client(file.name)

                state = self.state_store.get_state(file.name)
                if state and state[0] != file.last_modified.timestamp():
                    res = self.upload_files_to_cdf(file_client, file)
                    files_uploaded += 1
                    self.logger.info(
                        f"Uploaded file {file.name} to CDF with id {res.id}"
                    )
                    self.state_store.set_state(
                        file.name, file.last_modified.timestamp()
                    )
                    self.state_store.synchronize()

        if files_uploaded > 0:
            self.run_extraction_pipeline(
                status="success",
                message=f"Uploaded {files_uploaded} files to CDF from {file_path}",
            )

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

    def extract_time_series_data(self, file_path: str) -> None:
        token = self.azure_credential.get_token(
            "https://storage.azure.com/.default"
        ).token
        self.logger.debug(f"Extracting time series data from {file_path}")
        external_ids = self.retrieve_external_ids_from_lakehouse(file_path, token=token)
        self.logger.debug(f"External IDs found: {external_ids}")
        latest_timestamps = self.get_timeseries_latest_timestamps(external_ids)
        for external_id, timestamp in latest_timestamps.items():
            for time_series_data in self.convert_lakehouse_data_to_df_batch(
                file_path,
                external_id,
                timestamp,
                token=token,
            ):
                self.write_time_series_to_cdf(external_id, time_series_data)

    def get_timeseries_latest_timestamps(self, external_ids: list[str]) -> dict:
        state_ids = [
            f"{self.config.source.raw_time_series_path}-{external_id}-state"
            for external_id in external_ids
        ]
        states = self.state_store.get_state(state_ids)
        latest_timestamps = dict(zip(external_ids, [state[0] for state in states]))
        return latest_timestamps

    def write_time_series_to_cdf(self, external_id: str, data_frame: DataFrame) -> None:
        self.logger.info(f"Writing time series data to CDF for {external_id}")
        state_id = f"{self.config.source.raw_time_series_path}-{external_id}-state"
        latest_process_time = data_frame["timestamp"].max()
        data_frame.loc[:, "externalId"] = data_frame.apply(
            lambda row: self.config.destination.time_series_prefix + row["externalId"],
            axis=1,
        )
        data_frame = data_frame.pivot(
            index="timestamp", columns="externalId", values="value"
        )
        data_frame.index = pd.to_datetime(data_frame.index)
        try:
            self.client.time_series.data.insert_dataframe(data_frame)
            self.logger.debug(f"Time series data written to CDF for {external_id}")
            self.set_state(state_id, str(latest_process_time))
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
            try:
                self.client.time_series.data.insert_dataframe(data_frame)
                self.logger.debug(f"Time series data written to CDF for {external_id}")
                self.set_state(state_id, str(latest_process_time))
            except CogniteAPIError as e:
                self.logger.error(f"Error while writing time series data to CDF: {e}")
                raise e
        except CogniteAPIError as e:
            self.logger.error(f"Error while writing time series data to CDF: {e}")
            raise e

    def write_event_data_to_cdf(
        self,
        file_path: str,
        token: str,
        state_id: str,
        incremental_field: str | None,
    ) -> None:
        state = str(self.state_store.get_state(state_id)[0])

        for df in self.convert_lakehouse_data_to_df_batch_filtered(
            file_path, token, state, ["externalId"], False, incremental_field
        ):
            if len(df) > 0:
                events = self.get_events(df)

                try:
                    self.client.events.upsert(events)
                    self.run_extraction_pipeline(
                        status="success",
                        message=f"{len(events)} events inserted to CDF",
                    )
                except CogniteAPIError as e:
                    self.logger.error(f"Error while writing event data to CDF: {e}")
                    self.run_extraction_pipeline(
                        status="failure",
                        message=f"Error while writing event data to CDF: {e}",
                    )
                    raise e

                self.set_state(state_id, df[incremental_field].max())
            else:
                self.run_extraction_pipeline(status="seen")

    def write_raw_tables_to_cdf(
        self,
        file_path: str,
        token: str,
        state_id: str,
        table_name: str,
        db_name: str,
        key_fields: List[str] | None,
        md5_key: bool,
        incremental_field: str | None,
    ) -> None:
        state = str(self.state_store.get_state(state_id)[0])
        for df in self.convert_lakehouse_data_to_df_batch_filtered(
            file_path, token, state, key_fields, md5_key, incremental_field
        ):
            if len(df) > 0:
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

                self.run_extraction_pipeline(
                    status="success", message=f"{len(df)} rows inserted to {table_name}"
                )
                self.set_state(state_id, df[incremental_field].max())

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
                end_time=row[1]["endTime"] if "endTime" in row[1] else None,
                type=row[1]["type"] if "type" in row[1] else None,
                subtype=row[1]["subtype"] if "subtype" in row[1] else None,
                metadata=row[1]["metadata"] if "metadata" in row[1] else None,
                description=row[1]["description"] if "description" in row[1] else None,
                asset_ids=self.get_asset_ids(row[1]["assetExternalIds"])
                if "assetExternalIds" in row[1]
                else None,
                data_set_id=self.data_set_id if self.data_set_id else None,
            )

            events.append(new_event)
        return events

    def retrieve_external_ids_from_lakehouse(
        self, file_path: str, token: str
    ) -> list[str]:
        try:
            dt = DeltaTable(file_path, storage_options={"bearer_token": token})
            table = dt.to_pyarrow_table(columns=["externalId"])
            return pc.unique(table.column("externalId")).to_pylist()

        except Exception as e:
            self.logger.error(
                f"Error while converting lakehouse data to DataFrame: {e}"
            )
            raise e

    def convert_lakehouse_data_to_df_batch_filtered(
        self,
        file_path: str,
        token: str | None,
        state: str,
        key_fields: List[str] | None,
        md5_key: bool,
        incremental_field: str | None,
    ) -> Generator[DataFrame, None, None]:
        has_state = state is not None and state != "None"
        try:
            if has_state and incremental_field:
                try:
                    date_state = datetime.strptime(state, "%Y-%m-%d %H:%M:%S.%f%z")
                except ValueError:
                    try:
                        date_state = datetime.strptime(
                            f"{state} 00:00:00.000Z", "%Y-%m-%d %H:%M:%S.%f%z"
                        )
                    except ValueError:
                        date_state = datetime(
                            1970, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc
                        )

                dt = DeltaTable(
                    file_path,
                    storage_options={
                        "bearer_token": str(token),
                        "use_fabric_endpoint": str(
                            self.config.extractor.use_fabric_endpoint
                        ).lower(),
                    },
                )
                dataset = dt.to_pyarrow_dataset()
                condition = ds.field(incremental_field) > date_state
                batch_set = dataset.sort_by(incremental_field).to_batches(
                    filter=condition, batch_size=self.config.source.read_batch_size
                )

            elif incremental_field:
                dt = DeltaTable(
                    file_path,
                    storage_options={
                        "bearer_token": str(token),
                        "use_fabric_endpoint": str(
                            self.config.extractor.use_fabric_endpoint
                        ).lower(),
                    },
                )
                batch_set = (
                    dt.to_pyarrow_dataset()
                    .sort_by(incremental_field)
                    .to_batches(batch_size=self.config.source.read_batch_size)
                )
            else:
                dt = DeltaTable(
                    file_path,
                    storage_options={
                        "bearer_token": str(token),
                        "use_fabric_endpoint": str(
                            self.config.extractor.use_fabric_endpoint
                        ).lower(),
                    },
                )
                batch_set = dt.to_pyarrow_dataset().to_batches(
                    batch_size=self.config.source.read_batch_size
                )

            for batch in batch_set:
                if len(batch) > 0:
                    self.logger.debug(f"Retrieved batch with {len(batch)} rows")
                    df = batch.to_pandas()
                    # Convert dates to string
                    for column in df.columns:
                        if pd.api.types.is_datetime64_any_dtype(df[column]):
                            df[column] = df[column].dt.strftime(
                                "%Y-%m-%d %H:%M:%S.%f%z"
                            )
                        elif pd.api.types.is_object_dtype(df[column]):
                            df[column] = df[column].astype(str)

                    # Remove NaT and NAN
                    df = df.replace({np.nan: None})

                    if md5_key:
                        df["md5_key"] = self.get_md5_series_from_dataframe(
                            df, key_fields
                        )
                        df.set_index("md5_key", inplace=True)
                    #                    elif incremental_field:
                    #                         df.set_index(key_fields, inplace=True)

                    yield df

        except Exception as e:
            self.logger.error(
                f"Error while converting lakehouse data to DataFrame: {e}"
            )
            raise e

    def convert_lakehouse_data_to_df_batch(
        self, file_path: str, external_id: str, timestamp: str, token: str
    ) -> Generator[DataFrame, None, None]:
        try:
            dt = DeltaTable(file_path, storage_options={"bearer_token": token})
            dataset = dt.to_pyarrow_dataset()
            condition = ds.field("externalId") == external_id
            if timestamp:
                timestamp_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f%z")
                condition = condition & (ds.field("timestamp") > timestamp_obj)
            batch_set = dataset.to_batches(
                filter=condition, batch_size=self.config.source.read_batch_size
            )
            for batch in batch_set:
                self.logger.debug(f"Retrieved batch with {len(batch)} rows")
                yield batch.to_pandas()
        except Exception as e:
            self.logger.error(
                f"Error while converting lakehouse data to DataFrame: {e}"
            )
            raise e

    def run_extraction_pipeline(
        self, status: Literal["success", "failure", "seen"], message: str = ""
    ) -> None:
        if self.config.cognite.extraction_pipeline:
            try:
                self.cognite_client.extraction_pipelines.runs.create(
                    ExtractionPipelineRunWrite(
                        status=status,
                        extpipe_external_id=str(
                            self.config.cognite.extraction_pipeline.external_id
                        ),
                        message=message,
                    )
                )
            except CogniteAPIError as e:
                self.logger.error(f"Error while running extraction pipeline: {e}")
                raise e

    def get_md5_from_series(self, input_iterable: Iterable) -> str:
        """
        Create a MD5 hash from an Iterable, typically a row from a Pandas ``DataFrame``, but can be any
        Iterable object instance such as a list, tuple or Pandas ``Series``.

        Args:
            input_iterable: Typically a Pandas ``DataFrame`` row, but can be any Pandas ``Series``.

        Returns:
            MD5 hash created from the input values.
        """
        # convert all values to string, concantenate, and encode so can hash
        full_str = "-".join(map(str, input_iterable)).encode("utf-8")

        # create a md5 hash from the complete string
        md5_hash = md5(full_str).hexdigest()

        return md5_hash

    def get_md5_series_from_dataframe(
        self, input_dataframe: pd.DataFrame, columns: Optional[Iterable[str]] = None
    ) -> pd.Series:
        """
        Create a Pandas ``Series`` of MD5 hashses for every row in a Pandas ``DataFrame``.

        Args:
            input_dataframe: Pandas ``DataFrame`` to be create MD5 hashes for.
            columns: If only wanting to use specific columns to calculate the hash, specify these here.

        Returns:
            MD5 hashes, one for every row in the input Pandas ``DataFrame``.
        """

        # if columns specified, filter to just these columns
        in_df = (
            input_dataframe.iloc[:, list(columns)]
            if columns is not None
            else input_dataframe
        )

        # create md5 hash per row
        md5_hashes = in_df.apply(lambda row: self.get_md5_from_series(row), axis=1)

        return md5_hashes

    def _report_success(self) -> None:
        """
        Called on a successful exit of the extractor - get rid of "Shutdown success" message that spams the logs.
        """
        pass
