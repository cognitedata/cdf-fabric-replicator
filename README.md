# CDF Fabric replicator

Application which utilize the CDF APIs to replicate data to Microsoft Fabric

Before running the extractor you need to setup a data point subscriptions, see the [SDK documentation](https://cognite-sdk-python.readthedocs-hosted.com/en/latest/core_data_model.html#create-data-point-subscriptions)

Configuration file:
```
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
        local:
            path: state.json
    subscription-batch-size: 10000
    ingest-batch-size: 100000
    poll-time: 5

# subscriptions to stream
subscriptions:
  - externalId: ts-subscription
    partitions:
        - 0
    lakehouse_abfss_path_dps: ${LAKEHOUSE_ABFSS_PREFIX}/Tables/${DPS_TABLE_NAME}
    lakehouse_abfss_path_ts: ${LAKEHOUSE_ABFSS_PREFIX}/Tables/${TS_TABLE_NAME}

# sync data model
data_modeling:
  - space: cc_plant
    lakehouse_abfss_prefix: ${LAKEHOUSE_ABFSS_PREFIX}

```
