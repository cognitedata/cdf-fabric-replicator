import pytest
from cognite.extractorutils.configtools import load_yaml
from cdf_fabric_replicator.config import Config


@pytest.fixture(scope="session")
def config_raw():
    yield """
    # Logging configuration
    logger:
        console:
            level: INFO

    # Cognite project to stream your datapoints from
    cognite:
        host: https://api.cognitedata.com
        project: unit_test_extractor

        idp-authentication:
            token-url: https://get-a-token.com/token
            client-id: abc123
            secret: def567
            scopes:
                - https://api.cognitedata.com/.default
        extraction-pipeline:
            external-id: test-fabric-extractor

    #Extractor config
    extractor:
        state-store:
            local:
                path: test_states.json
        subscription-batch-size: 10000
        ingest-batch-size: 100000
        poll-time: 5

    source:
        abfss-prefix: abfss://test_container@test_accout.dfs.core.windows.net
        event_path: source_table_path
        file_path: file_path
        raw_time_series_path: /table/path
        data_set_id: "123456"

    destination:
        type: test_events
        time_series_prefix: test_ts_

    #Event Replicator config
    event:
        batch-size: 1000
        lakehouse_abfss_path_events: abfss://test_container@test_accout.dfs.core.windows.net/Tables/Events
    """


@pytest.fixture(scope="session")
def config(config_raw):
    yield load_yaml(config_raw, Config)
