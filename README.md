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

The time series replicator uses [data point subscriptions](https://cognite-sdk-python.readthedocs-hosted.com/en/latest/time_series.html#create-data-point-subscription) to get updates on incoming time series data.  If only one subscription with a single partition is defined in the configuration file then the subscription is created automatically by the Time Series Replicator, based on the values in the configuration file.

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

## Command-line

Once Poetry is installed, navigate to the root directory of your project in your terminal.

Next, run the following command to install the project dependencies:
```
poetry install
```

Finally, run the replicator:
```
poetry run cdf_fabric_replicator config.yaml
```

## Visual Studio Code

Alternatively, if you are using Visual Studio Code, just open the folder of the root directory of your project.

You must still install Poetry as mentioned above in addition to the [Python Extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python) for VSCode.

The included ".vscode/launch.json" file will add "poetry install" and "poetry run" to your "Run and Debug" tab. They can be used in place of the manual command line statements above, and will attach the VSCode debugger.

For more information see [Debugging in Visual Studio Code](https://code.visualstudio.com/Docs/editor/debugging).

# Building and Deploying with Docker on AKS

## Pre-requisites

### Set Target Platform Arhcitecture

If you are running the Docker Container on ARM Architecture, you will need to use platform emulation for the Docker run command. You can do this by running the following command:

```
docker run -i -t <image-name> --platform linux/arm64 
```

### Creating an AKS Cluster with Managed Identity

Azure Kubernetes Service (AKS) can use Azure Managed Identities to interact with other Azure services. This eliminates the need to manage service principals and rotate credentials. The Fabric Replicator requires managed identity on AKS to be enabled to run.

To create an AKS cluster with Managed Identity and an attached ACR, you can use the Azure CLI:

```bash
az aks create -g MyResourceGroup -n MyManagedCluster --generate-ssh-keys --attach-acr MyACR --enable-managed-identity
```

### Build Docker to Push to ACR

Note: When you build the CDF Fabric Replicator from docker, the docker image is configured to use the config located in the Extraction Pipeline in CDF. You will need to configure the yaml file in the Extraction Pipeline in CDF.

First, you should build your your docker image locally:

```bash
docker build -t <MyImageName> -f ./build/Dockerfile . 
```

Before you can push an image to your ACR, you need to tag it with the login server name of your ACR instance:

```bash
docker tag <MyImageName>:latest <myregistry>.azurecr.io/<MyImageName>:latest
```

Next, log in to your ACR:

```bash
az acr login --name <myregistry>
```

Finally, push your image to your ACR instance:

```bash
docker push <myregistry>.azurecr.io/<MyImageName>:latest
```

Now, your Docker image is available in your ACR and can be pulled from your AKS cluster.

## Use helm to deploy your container to AKS

### Configure values.yaml

The `values.yaml` file is a key part of your Helm chart as it allows you to set the default configuration values for the chart. These values can be overridden by users during installation or upgrade.

Here's a step-by-step guide on how to configure your `values.yaml` file:

1. Open the `values.yaml` file in your chart directory.

2. Set the default values for your chart. 

Some key values you need to fill out are:

#### Image Repository

```yaml
image:
  repository: <ACRREPOSITORYNAME>.azurecr.io/<IMAGENAME>
  pullPolicy: Always
  # Overrides the image tag whose default is the chart appVersion.
  tag: "latest"
```

#### Environment Variables
The environmental variables should be filled in with values that correspond to your CDF and Lakehouse environment, these allow the CDF Fabric Replicator to run.

```Yaml
env:
  COGNITE_TOKEN_URL: ""
  COGNITE_CLIENT_ID: ""
  COGNITE_CLIENT_SECRET: ""
  COGNITE_TOKEN_SCOPES: ""
  COGNITE_CLIENT_NAME: ""
  COGNITE_PROJECT: ""
  COGNITE_BASE_URL: ""
  COGNITE_STATE_TABLE: ""
  LAKEHOUSE_TIMESERIES_TABLE: TimeSeries
  LAKEHOUSE_ABFSS_PREFIX: ""
  EXTRACTOR_EVENT_PATH: "Tables/RawEvents"
  EXTRACTOR_FILE_PATH: ""
  EXTRACTOR_RAW_TS_PATH: "Tables/RawTS"
  EXTRACTOR_DATASET_ID: ""
  EXTRACTOR_TS_PREFIX: ""
  EXTRACTOR_DESTINATION_TYPE: ""
  EVENT_TABLE_NAME: ""
  COGNITE_STATE_DB: ""
  COGNITE_EXTRACTION_PIPELINE: ""
  ```

### Connect to your AKS cluster

1. Login to your azure account:

    ```bash
    az login
    ```

2. Set the cluster subscription:

    ```bash
    az account set --subscription <Subscription ID>
    ```

3. Download cluster credentials

    ```bash
    az aks get-credentials --resource-group <RESOURCE GROUP> --name <ASK CLUSTER NAME> --overwrite-existing
    ```

### Install Via Helm

To deploy your application to your Kubernetes cluster run the following command:

```bash
helm install <MYAppName> ./cdf-fabric-replicator-chart
```

To verify the status of your deployment run:

```bash
kubectl get pods
```
