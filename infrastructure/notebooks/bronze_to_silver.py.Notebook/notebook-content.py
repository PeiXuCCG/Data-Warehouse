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

#!/usr/bin/env python
# coding: utf-8

# ## bronze_to_silver_rerun.py
# 
# New notebook

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[28]:


# Welcome to your new notebook
# Type here in the cell editor to add code!
#!/usr/bin/env python
# coding: utf-8

# ## Master_Linked_Demo.py
# 
# Demonstration of linking base data with master data using MasterLinkedTable
# and applying SCD Type 2 logic.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[1]:

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from typing import List, Tuple, Callable
from loom.tables  import MasterLinkedTable
from loom.pipelines import Pipeline
import sys
from notebookutils import mssparkutils
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[29]:

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

# CELL ********************

# In[30]:

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[3]:
source_lakehouse = "lh_bronze"
source_schema = "bronze"

target_dwh = "dwh_silver"
target_schema = "silver"

bc_prefix = "bc_"
historical_prefix = "historical_"

masterObjects = ["customer"]

table = "customer"
partition_key = "originating_company"
primary_key = "customer_hk"


dry_run = False
is_warehouse = True #this is used by loom as a switch to use T-SQL

workspace_name = mssparkutils.env.getWorkspaceName()


#

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[31]:


spark.conf.set("spark.datawarehouse.dwh_silver.sqlendpoint", "d3mzclqk6fqejkhqg6rot34see-n5wu7ldnbuseznlousn27ddxay.datawarehouse.fabric.microsoft.com")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[32]:


target_table = table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[33]:


pipeline_name = f"bronze_to_silver_{target_table}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[34]:


master_links = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[35]:


# In[7]:
# LOAD your master data tables here (maybe this is a list of spark.read.table)
exclude_patterns = ["_master_hk", "_hk", "reason", "effectivity_start_date", "effectivity_end_date"]


for object in masterObjects:
   masterlist_df = spark.sql(f" SELECT * FROM  {target_dwh}.master.masterlist where object = '{object}'")

   row = masterlist_df.collect()[0]
   
   # get the keys from the first row
   #business_key = row.businesskey.replace(" ", "")
   key = row.primarykey

   masterdata_df = spark.sql(f"SELECT * FROM {target_dwh}.master.{object}_master")

   materialized_columns = [
      col.strip() for col in masterdata_df.columns
      if not any(pattern in col for pattern in exclude_patterns)
   ]

   master_links.append(
      # master key, dataframe, key, columns
      (f"{object}_master_hk", masterdata_df, key, materialized_columns)
   )


# In[36]:

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
    history_df = spark.read.table(historical_table).drop("unmapped")
    

if not history_df.isEmpty():
    source_df = bc_df.unionByName(history_df, allowMissingColumns=True)
else:
    source_df = bc_df    


# In[37]:

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

existing_df = spark.createDataFrame(
    spark.sparkContext.emptyRDD(),
    StructType()
)
if spark.catalog.tableExists(f"{target_dwh}.{target_schema}.{target_table}"):
    existing_df = spark.read.synapsesql(f"{target_dwh}.{target_schema}.{target_table}")


# In[38]:

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
    existing_df = existing_df,
    schema_evolution=True,
    target_path="N/A", #Not supported in Fabric
    target_db=target_schema,
    target_schema=target_dwh,
    business_keys=[partition_key],
    primary_key=[primary_key],
    master_links=master_links,
    is_warehouse=True,
    spark=spark
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[39]:


masterlinked_table.prepare()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[41]:


masterlinked_table.df.write.mode("overwrite").synapsesql(f"{target_dwh}.{target_schema}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[42]:


print("✅ MasterLinkedTable completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[43]:


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
