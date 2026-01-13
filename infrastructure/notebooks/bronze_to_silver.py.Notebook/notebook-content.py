# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "059ddf57-7cf2-401b-8bf8-68beddef9667",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "0380b4e3-57d1-4574-abb4-3f7e7e8427d0",
# META       "known_lakehouses": [
# META         {
# META           "id": "059ddf57-7cf2-401b-8bf8-68beddef9667"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "19ef04e9-33e6-8865-4282-9c499f72e816",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
#!/usr/bin/env python
# coding: utf-8

# ## Master_Linked_Demo.py
# 
# Demonstration of linking base data with master data using MasterLinkedTable
# and applying SCD Type 2 logic.

# In[1]:

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import List, Tuple, Callable
from loom.tables  import MasterLinkedTable
from loom.pipelines import Pipeline
import sys
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[2]:
spark = SparkSession.builder.appName("SilverTableMasterData").getOrCreate()
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# In[3]:
source_lakehouse = "lh_bronze"
source_schema = "bronze"

target_dwh = "dwh_silver"
target_schema = "silver"

bc_prefix = "bc_"
historical_prefix = "historical_"

masterObjects = []

table = "item"
partition_key = "itemcode"
primary_key = "item_hk"


dry_run = True
is_warehouse = True #this is used by loom as a switch to use T-SQL

workspace_name = mssparkutils.env.getWorkspaceName()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table = table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pipeline_name = f"bronze_to_silver_{target_table}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[4]:
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{target_dwh}`.`{target_schema}`")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

master_links = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[7]:
# LOAD your master data tables here (maybe this is a list of spark.read.table)
exclude_columns = {
    "masterkey",
    "primarykeyfield",
    "primarykeyvalue",
    "reason",
    "effectivity_start_date",
    "effectivity_end_date",
}

for object in masterObjects:
   masterlist_df = spark.read.synapsesql(f"{target_dwh}.{target_schema}.masterdata where object = '{object}'")

   row = masterlist_df.collect()[0]
   
   # get the keys from the first row
   business_key = row.business_key
   primary_key = row.primary_key

   masterdata_df = spark.read.synapsesql(f"{target_dwh}.{target_schema}.{object}_master")

   materialized_columns = [
        col for col in masterdata_df.columns
        if col not in exclude_columns
   ]

   master_links.append(
      # master key, dataframe, businesskey, primarykey, columns
      (f"{object}_master_hk", masterdata_df, business_key, primary_key, materialized_columns)
   )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bc_table = f"{source_lakehouse}.{source_schema}.{bc_prefix}{table}"
historical_table = f"{source_lakehouse}.{source_schema}.{historical_prefix}{table}"

if spark.catalog.tableExists(bc_table):
    bc_df = spark.read.table(bc_table)

if spark.catalog.tableExists(historical_table):
    history_df = spark.read.table(historical_table)
    

if not history_df.isEmpty():
    source_df = bc_df.unionByName(history_df)
else:
    source_df = bc_df    



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# In[ ]:
pipelines = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[9]:
# Instantiate MasterLinkedTable and prepare data
masterlinked_table = MasterLinkedTable(
    name=target_table,
    df=source_df,
    schema_evolution=True,
    target_path="N/A", #Not supported in Fabric
    target_db=target_schema,
    target_schema=target_dwh,
    business_keys=[partition_key],
    primary_key=[primary_key],
    master_links=master_links,
    is_warehouse=True
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[12]:
pipeline = Pipeline(
    name=pipeline_name,
    tables=[masterlinked_table],
    dry_run=dry_run,  # Set to True to simulate without writing
    target_schema=target_schema,
    target_db="dbo"
)

pipelines.append(pipeline)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[12]:
for p in pipelines:
    p.summary()
    p.validate()
    p.execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[12]:
print("✅ MasterLinkedTable completed successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if "DEV" in workspace_name: #DEV (when on the trial)
    try:
        spark.stop()
    except:
        pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
