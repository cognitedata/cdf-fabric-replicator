#!/usr/bin/python3
import os
from configparser import ConfigParser

# This script will deploy the cdf-fabric-replicator container to Azure Container Instances.

# To configure the script, create a .env file with the following and define the values with the appropriate values for your environment:

# COGNITE_PROJECT=
# COGNITE_BASE_URL=
# COGNITE_TOKEN_URL=
# COGNITE_CLIENT_SECRET=
# COGNITE_CLIENT_ID=
# COGNITE_STATE_DB=
# COGNITE_STATE_TABLE=
# COGNITE_EXTRACTION_PIPELINE=
# LAKEHOUSE_ABFSS_PREFIX=
# DPS_TABLE_NAME=
# TS_TABLE_NAME=
# AZ_SUBSCRIPTION_ID=
# AZ_RESOURCE_GROUP=
# AZ_CONTAINER_NAME=
# AZ_LOCATION=
# AZ_MANAGED_ID_NAME=
# CR_LOGIN_SERVER=
# CR_IMAGE=
# CR_VERSION=
# CR_USERNAME=
# CR_PERSONAL_ACCESS_TOKEN=

# The COGNITE_ variables are used to specify the settings for the Cognite API.
# The LAKEHOUSE_ variables are used to specify the settings for the target Fabric Onelake instance.
# The AZ_ variables are used to specify the settings for the target Azure Container Instance.
# The CR_ variables are used to specify the settings for the container registry for the pre-built container image.

config = ConfigParser()
with open(".env") as stream:
    config.read_string("[top]\n" + stream.read())

os.system(f"az account set --subscription {config.get('top', 'AZ_SUBSCRIPTION_ID')}")

os.system(
    f"az \
  container create \
  --resource-group {config.get('top', 'AZ_RESOURCE_GROUP')} \
  --name {config.get('top', 'AZ_CONTAINER_NAME')} \
  --image {config.get('top', 'CR_LOGIN_SERVER')}/{config.get('top', 'CR_IMAGE')}:{config.get('top', 'CR_VERSION')} \
  --registry-login-server {config.get('top', 'CR_LOGIN_SERVER')} \
  --registry-username {config.get('top', 'CR_USERNAME')} \
  --registry-password {config.get('top', 'CR_PERSONAL_ACCESS_TOKEN')} \
  --location {config.get('top', 'AZ_LOCATION')} \
  --environment-variables \
  COGNITE_PROJECT={config.get('top', 'COGNITE_PROJECT')} \
  COGNITE_BASE_URL={config.get('top', 'COGNITE_BASE_URL')} \
  COGNITE_TOKEN_URL={config.get('top', 'COGNITE_TOKEN_URL')} \
  COGNITE_CLIENT_ID={config.get('top', 'COGNITE_CLIENT_ID')} \
  LAKEHOUSE_ABFSS_PREFIX={config.get('top', 'LAKEHOUSE_ABFSS_PREFIX')} \
  DPS_TABLE_NAME={config.get('top', 'DPS_TABLE_NAME')} \
  TS_TABLE_NAME={config.get('top', 'TS_TABLE_NAME')} \
  COGNITE_EXTRACTION_PIPELINE={config.get('top', 'COGNITE_EXTRACTION_PIPELINE')} \
  COGNITE_STATE_DB={config.get('top', 'COGNITE_STATE_DB')} \
  COGNITE_STATE_TABLE={config.get('top', 'COGNITE_STATE_TABLE')} \
  --secure-environment-variables \
  COGNITE_CLIENT_SECRET={config.get('top', 'COGNITE_CLIENT_SECRET')} \
  --assign-identity /subscriptions/{config.get('top', 'AZ_SUBSCRIPTION_ID')}/resourceGroups/{config.get('top', 'AZ_RESOURCE_GROUP')}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{config.get('top', 'AZ_MANAGED_ID_NAME')}"
)

os.system(
    f"az container logs \
  --resource-group {config.get('top', 'AZ_RESOURCE_GROUP')} \
  --name {config.get('top', 'AZ_CONTAINER_NAME')} \
  --follow"
)
