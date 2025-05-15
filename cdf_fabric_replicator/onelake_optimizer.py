import logging

from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.base import CancellationToken
from cognite.extractorutils.base import Extractor
from azure.identity import DefaultAzureCredential
from deltalake import DeltaTable
from deltalake.exceptions import DeltaError
from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config
from cdf_fabric_replicator.metrics import Metrics


class OnelakeOptimizer(Extractor):
    def __init__(self, metrics: Metrics, stop_event: CancellationToken) -> None:
        super().__init__(
            name="cdf_fabric_replicator_onelake_optimizer",
            description="Onelake Optimizer",
            config_class=Config,
            metrics=metrics,
            use_default_state_store=False,
            version=__version__,
            cancellation_token=stop_event,
        )
        self.azure_credential = DefaultAzureCredential()
        self.stop_event = stop_event
        self.logger = logging.getLogger(self.name)

    def run(self) -> None:
        self.logger.info("Run Called for OneLake Optimizer...")
        sleep_time = self.config.extractor.optimize_poll_time

        while not self.stop_event.is_set():
            self.stop_event.wait(sleep_time)  # don't optimize on startup
            self.logger.info("Running OneLake Optimizer...")
            self.process_onelake_tables()

        self.logger.info("Stop event set. Exiting...")

    def process_onelake_tables(self) -> None:
        for data_model_config in self.config.data_modeling:
            try:
                all_views = self.cognite_client.data_modeling.views.list(
                    space=data_model_config.space, limit=-1
                )
            except CogniteAPIError as e:
                self.logger.error(
                    f"Failed to list views for space {data_model_config.space}. Error: {e}"
                )
                raise e

            views_dict = all_views.dump()

            for view in views_dict:
                table_folder = f"{data_model_config.lakehouse_abfss_prefix}/Tables/{data_model_config.space}_{view['externalId']}"
                self.optimize_table(table_folder)

    def optimize_table(self, abfss_path: str) -> None:
        token = self.azure_credential.get_token("https://storage.azure.com/.default")
        storage_options = {
            "bearer_token": token.token,
            "timeout": "1800s",
            # "use_fabric_endpoint": "true",
        }

        try:
            self.logger.info(f"Optimizing table: {abfss_path}")
            dt = DeltaTable(abfss_path, storage_options=storage_options)
            self.logger.debug(f"Compacting table: {abfss_path}")
            self.logger.debug(dt.optimize.compact(target_size=256 * 1024 * 1024))
            self.logger.debug(f"Vacuuming table: {abfss_path}")
            self.logger.debug(
                dt.vacuum(retention_hours=1, enforce_retention_duration=False)
            )
            dt.create_checkpoint()
            self.logger.debug("Done optimizing table.")
        except DeltaError as e:
            self.logger.error(f"Error optimizing table: {e}")
            # raise e
