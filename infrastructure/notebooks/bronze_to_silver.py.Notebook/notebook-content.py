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
# META       "environmentId": "39f1badd-f616-482b-9dc2-e9db7c8b2617",
# META       "workspaceId": "ac4f6d6f-0d6d-4c24-b56e-a49baf8c7706"
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
from pyspark.sql.types import StructType, DateType, TimestampType, StringType
,from typing import List, Tuple, Callable
from loom.tables  import MasterLinkedTable
from loom.pipelines import Pipeline
import sys
from notebookutils import mssparkutils
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
from functools import reduce

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

# PARAMETERS CELL ********************

# In[3]:
source_lakehouse = "lh_bronze"
source_schema = "bronze"

target_dwh = "dwh_silver"
target_schema = "silver"

bc_prefix = "bc_"
historical_prefix = "historical_"
contracts_prefix = "contract_"


table = "customer"
partition_key = "originating_company"
primary_key = "customer_hk"


dry_run = False
is_warehouse = True #this is used by loom as a switch to use T-SQL

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adding more master objects here
masterObjects = [
    "customer",
    "facility", 
    "fundingbody", 
    "custledgerentry", 
    "glaccount", 
    "glentry", 
    "item", 
    "itemledgerentry",
    "location", 
    "paymentterms", 
    "prescriber", 
    "purchinvheader",
    "purchinvline",
    "salesheader",
    "salesinvoiceheader",
    "salesline",
    "salespersonpurchaser",
    "vendor",
    "vendorledgerentry",
    # Tagging
    "brand",
    "customerchannel",
    "customertype"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_name = mssparkutils.env.getWorkspaceName()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[31]:
if "PROD" in workspace_name: 
    spark.conf.set("spark.datawarehouse.dwh_silver.sqlendpoint", "d3mzclqk6fqejkhqg6rot34see-nxxzyi4xhxuudjers5wfcrz3ce.datawarehouse.fabric.microsoft.com")
else:
    # UAT
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

   if masterlist_df.count() > 0:

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

tables = {
    "bc": f"{source_lakehouse}.{source_schema}.{bc_prefix}{table}",
    "history": f"{source_lakehouse}.{source_schema}.{historical_prefix}{table}",
    "contracts": f"{source_lakehouse}.{source_schema}.{contracts_prefix}{table}",
    # add more sources here later
    # "future": f"{source_lakehouse}.{source_schema}.{future_prefix}{table}",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cast_df_to_schema(source_df, target_df):
    target_schema = target_df.schema

    select_exprs = []
    for field in target_schema:
        name = field.name
        dtype = field.dataType

        if name in source_df.columns:
            select_exprs.append(F.col(name).cast(dtype).alias(name))
        else:
            select_exprs.append(F.lit(None).cast(dtype).alias(name))

    return source_df.select(*select_exprs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def normalize_all_dates_for_sql(df, exclude_cols=None):
    SQL_MIN_YEAR = 1753
    SQL_MAX_YEAR = 9999

    exclude_cols = set(exclude_cols or [])

    for field in df.schema.fields:
        c = field.name
        t = field.dataType

        if c in exclude_cols:
            continue

        # ----------------------------
        # DATE columns
        # ----------------------------
        if isinstance(t, DateType):
            year = F.year(F.col(c))

            df = df.withColumn(
                c,
                F.when(
                    F.col(c).isNull(), None
                ).when(
                    (year >= SQL_MIN_YEAR) & (year <= SQL_MAX_YEAR),
                    F.col(c)
                ).otherwise(F.lit(None).cast(DateType()))
            )

        # ----------------------------
        # TIMESTAMP columns
        # ----------------------------
        elif isinstance(t, TimestampType):
            year = F.year(F.col(c))

            df = df.withColumn(
                c,
                F.when(
                    F.col(c).isNull(), None
                ).when(
                    (year >= SQL_MIN_YEAR) & (year <= SQL_MAX_YEAR),
                    F.col(c).cast("timestamp")  # force precision normalization
                ).otherwise(F.lit(None).cast(TimestampType()))
            )

        # ----------------------------
        # STRING columns that look like dates
        # ----------------------------
        elif (
            isinstance(t, StringType)
            and any(c.lower().endswith(s) for s in ("date", "datetime", "timestamp", "at"))
        ):
            year = F.substring(F.col(c), 1, 4).cast("int")

            parsed = F.to_timestamp(
                F.col(c),
                "yyyy-MM-dd['T'HH:mm:ss[.SSSSSS][XXX]]"
            )

            df = df.withColumn(
                c,
                F.when(
                    F.col(c).isNull(), None
                ).when(
                    (year >= SQL_MIN_YEAR) & (year <= SQL_MAX_YEAR),
                    parsed
                ).otherwise(F.lit(None).cast(TimestampType()))
            )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -------------------------------------------------
# Load only tables that exist
# -------------------------------------------------
dfs = {}

for name, table_name in tables.items():
    if spark.catalog.tableExists(table_name):
        df = spark.read.table(table_name)

        # source-specific cleanup
        if name != "bc" and "unmapped" in df.columns:
            df = df.drop("unmapped") #don't need to carry the unmapped
 

        dfs[name] = df

# -------------------------------------------------
# Validate base source
# -------------------------------------------------
if "bc" not in dfs:
    print(f"WARNING: BC table not found: {tables['bc']}")
else:
    bc_df = dfs["bc"]
    for name, df in dfs.items():
        if name != "bc":
            #cast to bc schema
            dfs[name] = cast_df_to_schema(df, bc_df)

if not dfs:
    mssparkutils.notebook.exit(f"All Dataframes were empty for {target_table}")

# -------------------------------------------------
# Union all available sources
# -------------------------------------------------
source_df = normalize_all_dates_for_sql(reduce(
    lambda d1, d2: d1.unionByName(d2, allowMissingColumns=True),
    dfs.values()
))
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


masterlinked_table.df.write.mode("overwrite").option("overwriteSchema", "true").synapsesql(f"{target_dwh}.{target_schema}.{target_table}")

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
