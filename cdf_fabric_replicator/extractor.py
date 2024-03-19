import time
from urllib.parse import urlparse

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import (
    DataLakeServiceClient,
)
from cognite.client.data_classes import EventWrite, ExtractionPipelineRunWrite
from cognite.extractorutils import Extractor
from cognite.extractorutils.base import CancellationToken
from deltalake import DeltaTable

from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config


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

                res = self.cognite_client.files.upload_bytes(
                    file_client.download_file().readall(),
                    name=file.name.split("/")[-1],
                    external_id=file.name,
                    data_set_id=self.config.source.data_set_id if self.config.source.data_set_id else None,
                    source_created_time=int(file.creation_time.timestamp() * 1000),
                    source_modified_time=int(file.last_modified.timestamp() * 1000),
                    overwrite=True,
                )
                self.logger.info(f"Uploaded file {file.name} to CDF with id {res.id}")
                if self.config.cognite.extraction_pipeline:
                    self.cognite_client.extraction_pipelines.runs.create(
                        ExtractionPipelineRunWrite(
                            status="success",
                            extpipe_external_id=str(self.config.cognite.extraction_pipeline.external_id),
                        )
                    )
                self.state_store.set_state(file.name, file.last_modified.timestamp())
                self.state_store.synchronize()

    def run(self) -> None:
        self.config = self.get_current_config()
        self.client = self.config.cognite.get_cognite_client("cdf-fabric-extractor")
        self.state_store = self.get_current_statestore()
        self.state_store.initialize()

        # dataset = (
        #    self.client.data_sets.retrieve(external_id=self.config.cognite.data_set.either_id.external_id)
        #    if self.config.cognite.data_set
        #    else None
        # )
        # dataset_id = dataset.id if dataset else None

        if not self.config.source:
            self.logger.error("No source path or directory provided")
            return

        state_id = f"{self.config.source.abfss_path}-state"

        while self.stop_event.is_set() is False:
            token = self.azure_credential.get_token("https://storage.azure.com/.default")
            if self.config.cognite.extraction_pipeline:
                self.cognite_client.extraction_pipelines.runs.create(
                    ExtractionPipelineRunWrite(
                        status="seen", extpipe_external_id=str(self.config.cognite.extraction_pipeline.external_id)
                    )
                )

            if self.config.source.abfss_path:
                dt = DeltaTable(
                    self.config.source.abfss_path,
                    storage_options={"bearer_token": token.token, "user_fabric_endpoint": "true"},
                )
                df = dt.to_pandas()

                if str(self.state_store.get_state(state_id)[0]) != str(len(df)):
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

                    self.client.events.upsert(events)
                    if self.config.cognite.extraction_pipeline:
                        self.cognite_client.extraction_pipelines.runs.create(
                            ExtractionPipelineRunWrite(
                                status="success",
                                extpipe_external_id=str(self.config.cognite.extraction_pipeline.external_id),
                            )
                        )

                    self.state_store.set_state(state_id, str(len(df)))
                    self.state_store.synchronize()

            if self.config.source.abfss_directory:
                self.download_files_from_abfss(self.config.source.abfss_directory)

            # download_file_from_abfss(account_name, container_name, file_path, 'PATH_TO_SAVE_FILE')

            time.sleep(5)
