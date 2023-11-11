# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Enable the New Logging Schemas

# COMMAND ----------

import requests
metastore_id = "" # get from the accounts portal. take from the metastore url or get it programmatically

host = "https://"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
headers = {"Authorization": "Bearer "+dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}

# Fetch all available audit features for your metastore
r = requests.get(f"{host}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas", headers=headers).json()
schemas = r.get('schemas', [])

# Enable each audit feature audit-schema available to turn on
for schema in schemas:
    schema_name = schema['schema']
    try:
        r = requests.put(f"{host}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas/{schema_name}", headers=headers).json()
        if 'error_code' in r:
            print(f"Error while enabling schema {schema_name}: {r['error_code']}")
        else:
            print(f"Successfully enabled schema {schema_name}")
    except Exception as e:
        print(f"Exception while enabling schema {schema_name}: {str(e)}")
