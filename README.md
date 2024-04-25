# CDF Fabric replicator

Application which utilizes the CDF APIs to replicate data to and from Microsoft Fabric

# Replicator Services
The replicator consists of four services:
- **Time series replicator** - Copies time series data from CDF to Fabric
- **Data model replicator** - Copies data model nodes and edges from CDF to Fabric
- **Event replicator** - Copies event data from CDF to Fabric
- **Fabric data extractor** - Copies time series, events, and files from Fabric to CDF

All four services will run concurrently during the run of the CDF Fabric Replicator program.  The services use one state store in CDF's raw storage to maintain checkpoints of when the latest data was copied, so the services can be started and stopped and will be able to pick back up where they left off.

# Setting up Data Point Subscriptions

The time series replicator uses [data point subscriptions](https://cognite-sdk-python.readthedocs-hosted.com/en/latest/time_series.html#create-data-point-subscription) to get updates on incoming time series data.  Currently the only way to create these subscriptions is by using the Cognite SDK.

Here is an example of how to set up a subscription using the Python SDK:

First, install the SDK using `pip`:
```
pip install cognite-sdk
```
Next, set up the OAuth Credentials to use for authentication for the client.  You can get these values from an administrator:
```
from cognite.client.credentials import OAuthClientCredentials
import os

oauth_creds = OAuthClientCredentials(
    token_url="https://login.microsoftonline.com/xyz/oauth2/v2.0/token", # Auth token URL, replace "xyz" with Azure tenant ID
    client_id="abcd", # Client ID of the service principal for interacting with Cognite
    client_secret=os.environ["OAUTH_CLIENT_SECRET"], # Secret for the service principal, save as an environment variable as a best practice
    scopes=["https://greenfield.cognitedata.com/.default"], # Scope, contains cluster name for the CDF project
)
```
Create the Cognite Client using these credentials:
```
from cognite.client import CogniteClient, ClientConfig, global_config

cnf = ClientConfig(
  client_name="my-special-client",
  base_url="https://greenfield.cognitedata.com", # Base URL for CDF project, includes cluster name
  project="project-name", # CDF project name
  credentials=oauth_creds # OAuth credentials from earlier
)
global_config.default_client_config = cnf
client = CogniteClient()
```
Finally, create the subscriptions by referencing the external IDs of the time series to which you would like to subscribe:
```
from cognite.client.data_classes import DataPointSubscriptionWrite

sub = DataPointSubscriptionWrite(external_id="mySubscription", partition_count=1, time_series_ids=["myFistTimeSeries", "mySecondTimeSeries"], name="My subscription")
created = client.time_series.subscriptions.create(sub)
```
The external ID of the subscription (in this case, "mySubscription") will be used in the configuration file for the replicator.  For more specifics on the SDK, please refer to the [SDK documentation](https://cognite-sdk-python.readthedocs-hosted.com/en/latest/index.html).

# Environment Variables

You can optionally copy the contents of `.env.example` to a `.env` file that will be used to set the values in a config yaml file:

## CDF Variables
- `COGNITE_BASE_URL`: The base URL of the Cognite project, i.e. https://<cluster_name>.cognitedata.com
- `COGNITE_PROJECT`: The project ID of the Cognite project.
- `COGNITE_TOKEN_URL`: The URL to obtain the authentication token for the Cognite project, i.e. https://login.microsoftonline.com/<tenant_id>/oauth2/v2.0/token
- `COGNITE_CLIENT_ID`: The client ID for authentication with the Cognite project.
- `COGNITE_CLIENT_SECRET`: The client secret for authentication with the Cognite project.
- `COGNITE_STATE_DB`: The database in CDF raw storage where the replicator state should be stored.
- `COGNITE_STATE_TABLE`: The table in CDF raw storage where the replicator state should be stored.  The replicator will create the table if it does not exist.
- `COGNITE_EXTRACTION_PIPELINE`: The extractor pipeline in CDF for the replicator.  [Learn more about configuring extractors remotely](https://docs.cognite.com/cdf/integration/guides/interfaces/configure_integrations)

## Fabric Variables
- `LAKEHOUSE_ABFSS_PREFIX`: The prefix for the Azure Blob File System Storage (ABFSS) path.  Should match pattern `abfss://<workspace_id>@msit-onelake.dfs.fabric.microsoft.com/<lakehouse_id>`.  Get this value by selecting "Properties" on your Lakehouse Tables location and copying "ABFS path".
- `DPS_TABLE_NAME`: The name of the table where data point values and timestamps should be stored in Fabric.  The replicator will create the table if it does not exist.
- `TS_TABLE_NAME`: The name of the table where time series metadata should be stored in Fabric.  The replicator will create the table if it does not exist.
- `EVENT_TABLE_NAME`: The name of the table where event data should be stored in Fabric. The replicator will create the table if it does not exist.

## Fabric Extractor Variables
- `EXTRACTOR_EVENT_PATH`: The table path for the events table in a Fabric lakehouse.  It's the relative path after the ABFSS prefix (e.g. "Tables/RawEvents")
- `EXTRACTOR_FILE_PATH`: The single file path or directory of the files in a Fabric lakehouse. It's the relative path after the ABFSS prefix (e.g. "Files" or "Files/Tanks.png)
- `EXTRACTOR_RAW_TS_PATH`: The file path for the raw timeseries table in a Fabric lakehouse. It's the relative path after the ABFSS prefix (e.g. "Tables/RawTS")
- `EXTRACTOR_DATASET_ID`: Specifies the ID of the extractor dataset when the data lands in CDF.
- `EXTRACTOR_TS_PREFIX`: Specifies the prefix for the extractor timeseries when the data lands in CDF.

## Integration Test Variables
- `TEST_CONFIG_PATH`: Specifies the path to the test configuration file with which test versions of the replicator are configured.

# Config YAML
The replicator reads its configuration from a YAML file specified in the run command.  You can configure your own YAML file based on the one in `example_config.yaml` in the repo.  That configuration file uses the environment variables in `.env`, the configuration can also be set using hard coded values.

`subscriptions` and `data_modeling` configurations are a list, so you can configure multiple data point subscriptions or data modeling spaces to replicate into Fabric.

```
logger:
    console:
        level: INFO

# Cognite project to stream your datapoints from
cognite:
    host: ${COGNITE_BASE_URL}
    project: ${COGNITE_PROJECT}

    idp-authentication:
        token-url: ${COGNITE_TOKEN_URL}
        client-id: ${COGNITE_CLIENT_ID}
        secret: ${COGNITE_CLIENT_SECRET}
        scopes:
            - ${COGNITE_BASE_URL}/.default
    extraction-pipeline:
        external-id: ts-sub

#Extractor config
extractor:
    state-store:
        raw:
            database: ${COGNITE_STATE_DB}
            table: ${COGNITE_STATE_TABLE}
    subscription-batch-size: 10000
    ingest-batch-size: 100000
    poll-time: 5

# subscriptions to stream
subscriptions:
    - external_id: ts-subscription
      partitions:
          - 0
      lakehouse_abfss_path_dps: ${LAKEHOUSE_ABFSS_PREFIX}/Tables/${DPS_TABLE_NAME}
      lakehouse_abfss_path_ts: ${LAKEHOUSE_ABFSS_PREFIX}/Tables/${TS_TABLE_NAME}

# sync data model
data_modeling:
    - space: cc_plant
      lakehouse_abfss_prefix: ${LAKEHOUSE_ABFSS_PREFIX}

source:
    abfss_prefix: ${LAKEHOUSE_ABFSS_PREFIX}
    event_path: ${EXTRACTOR_EVENT_PATH}
    file_path: ${EXTRACTOR_FILE_PATH}
    raw_time_series_path: ${EXTRACTOR_RAW_TS_PATH}
    data_set_id: ${EXTRACTOR_DATASET_ID}

destination:
    type: ${EXTRACTOR_DESTINATION_TYPE}
    time_series_prefix: ${EXTRACTOR_TS_PREFIX}

# sync events
event:
    lakehouse_abfss_path_events: ${LAKEHOUSE_ABFSS_PREFIX}/Tables/${EVENT_TABLE_NAME}
    batch_size: 5
```

# Poetry

To run the `cdf_fabric_replicator` application, you can use Poetry, a dependency management and packaging tool for Python.

First, make sure you have Poetry installed on your system. If not, you can install it by following the instructions in the [Poetry documentation](https://python-poetry.org/docs/#installation).

Once Poetry is installed, navigate to the root directory of your project in your terminal.

Next, run the following command to install the project dependencies:
```
poetry install
```

Finally, run the replicator:
```
poetry run cdf_fabric_replicator config.yaml
```

# Docker

If you are running the Docker Container on ARM based Macs. You will need to use platform emulation for the Docker run command. You can do this by running the following command:

```
docker run -i -t <image-name> --platform linux/arm64 
```