# Databricks notebook source
pip install azure-mgmt-datafactory

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
import os

# COMMAND ----------

SUBSCRIPTION_ID = "your-subscription-id"
RESOURCE_GROUP = "your-resource-group"
DATA_FACTORY_NAME = "your-adf-name"
PIPELINE_NAME = "your-pipeline-name"

# COMMAND ----------

credential = DefaultAzureCredential()
adf_client = DataFactoryManagementClient(credential, SUBSCRIPTION_ID)

# COMMAND ----------

response = adf_client.pipelines.create_run(RESOURCE_GROUP, DATA_FACTORY_NAME, PIPELINE_NAME)
print(f"Pipeline Run ID: {response.run_id}")  