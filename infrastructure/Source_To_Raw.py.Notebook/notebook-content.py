# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3f1f512f-42fd-421b-b609-0bf88b0310aa",
# META       "default_lakehouse_name": "ccg_bronze",
# META       "default_lakehouse_workspace_id": "58ec2dce-fe1c-4c33-bde7-923914528ace",
# META       "known_lakehouses": [
# META         {
# META           "id": "3f1f512f-42fd-421b-b609-0bf88b0310aa"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "1e90633d-7747-4031-8cd8-68323cec79cc",
# META       "workspaceId": "58ec2dce-fe1c-4c33-bde7-923914528ace"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from loom.tables.table_type  import TableType
from loom.tables.plain_table import PlainTable
from loom.pipelines import Pipeline
from pyspark.sql.functions import lit
import re
from notebookutils import mssparkutils
import sys

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite","LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

target_schema = "ccg_bronze"
target_db =  "raw" 
source_system = "BC"
source_entity = ""
target_table = f"State"
source_path = 'Files/deltas/County-28004'
is_multi_line = True
pipeline_name = f"{source_system}_{target_table}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{target_schema}`.`{target_db}`")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ingestion_log = "dbo.ingestion_log"
pipelines = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cleanse(df):
    #remove spaces
    pattern = r"[^a-zA-Z0-9_]+" 
    new_columns = [re.sub(pattern, "", col_name).split("-")[0].lower() for col_name in df.columns]
    df_cleaned = df.toDF(*new_columns)

    return df_cleaned

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# Prevent duplicate loads
files = [f.path for f in mssparkutils.fs.ls(source_path) if f.name.endswith(".csv")]

# Step 2: Read existing file log
log_df = None
if spark.catalog.tableExists(ingestion_log):
    log_df = spark.read.table(ingestion_log)
    print("Table loaded successfully.")
else:
    print(f"Table {ingestion_log} does not exist.")

if log_df is not None:
    loaded_files = [r["source_file"] for r in log_df.collect()]

    # Step 3: Filter new files
    new_files = [f for f in files if f not in loaded_files]
else:
    # Step 3: first load
    new_files = files

if not new_files:
    print("✅ No new files to load, exit")
    sys.exit()
else:
    print(f"📂 Loading {len(new_files)} new files...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("header", True).option("inferSchema", True).option("multiLine", is_multi_line).csv(new_files)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not source_system == 'BC':
    df = df.withColumn("Company", lit(source_entity))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers_raw = PlainTable(
    target_db=target_db,
    target_schema=target_schema,
    name=target_table,
    df=df,
    source_system=source_system,
    target_path="NOT_SUPPORTED_YET", # this is technically not used due to fabric not supporting it but leave it here
    write_method="overwrite",
    cleanse_function=cleanse
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pipeline = Pipeline(
    name=pipeline_name,
    tables=[customers_raw],
    dry_run=False,  # Set to True to simulate without writing
    target_schema=target_schema,
    target_db="dbo" # this for audit logs
)

pipelines.append(pipeline)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
