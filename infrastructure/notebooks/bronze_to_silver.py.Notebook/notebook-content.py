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
from tables  import MasterLinkedTable
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
source_table = "bc_item"


partition_key = "itemcode"
primary_key = "item_hk"

target_dwh = "dwh_silver"
target_schema = "silver"
target_table = "bc_item"
pipeline_name = f"bronze_to_silver_{target_table}"
dry_run = True
is_warehouse = True

workspace_name = mssparkutils.env.getWorkspaceName()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# In[4]:
spark.synapsesql(f"CREATE SCHEMA IF NOT EXISTS `{target_dwh}`.`{target_schema}`")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[7]:
# LOAD your master data tables here (maybe this is a list of spark.read.table)
item_master_df = spark.read.synapsesql(f"{target_dwh}.{target_schema}.item_master")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[8]:
# Define add the above spark.read.tables to a dictionary and how it is linked to the resulting dwh table
master_links = [
    #("product_master_hk", product_master_df, "productcode", "product_hk", [{"ProductName": "product_name"}])
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_df = spark.read.table(f"{source_lakehouse}.{source_schema}.{source_table}")

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
sales_table = MasterLinkedTable(
    name=target_table,
    df=source_df,
    schema_evolution=True,
    target_path="", #Not supported in Fabric
    target_db=target_schema,
    target_schema=target_dwh,
    business_keys=[business_key],
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
    tables=[sales_table],
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
print("✅ MasterLinkedTable demo completed successfully.")


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
