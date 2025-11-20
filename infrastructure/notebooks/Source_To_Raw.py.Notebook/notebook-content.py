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

from pyspark.sql import SparkSession
from loom.tables.table_type  import TableType
from loom.tables.plain_table import PlainTable
from loom.pipelines import Pipeline
from pyspark.sql.functions import lit, concat_ws, col, sha2
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

target_schema = "lh_bronze" # lakehouse
target_db =  "raw"  # db schema
source_system = "BC"# data source
source_entity = "" # the company
target_table = f"bc_custledgerentry"
source_path = 'Files/deltas/CustLedgerEntry-21'
is_multi_line = True
pipeline_name = f"{source_system}_{target_table}"
write_method = "overwrite"
infer_schema = True

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
    #remove spaces and special characters
    pattern = r"[^a-z0-9_]+" 
    remove_dashes_columns = [col_name.split("-")[0].lower() for col_name in df.columns]
    new_columns = [re.sub(pattern, "", col_name) for col_name in remove_dashes_columns if col_name != "predictionconfidence"]
    df_cleaned = df.toDF(*new_columns)

    # call cleanse engine here

    return df_cleaned

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def path_exists(path):
    try:
        mssparkutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            # Re-raise other exceptions if they are not related to file not found
            raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prevent duplicate loads of files in directory
if path_exists(source_path):
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
        mssparkutils.notebook.exit(f"No new files to load for {source_entity}")
    else:
        print(f"📂 Loading {len(new_files)} new files...")
else:
   mssparkutils.notebook.exit(f"Path doesn't exist for {source_entity}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def prevent_duplicate_data(df):
    exclude_cols = ["systemmodifiedat","systemmodifiedby","delivereddatetime","ingestion_timestamp"]  # Add more if needed 
    #***(we want to keep if there are changes in the source system for a record) *** #
    
    cols_to_hash = [c for c in df.columns if c not in exclude_cols]
    
    concat_cols = concat_ws("||", *[col(c).cast("string") for c in cols_to_hash])
    df_hashed = df.withColumn("row_hash", sha2(concat_cols, 256))
    
    df_deduped = df_hashed.dropDuplicates(["row_hash"]).drop("row_hash")
    return df_deduped

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("header", True).option("inferSchema", infer_schema).option("multiLine", is_multi_line).option("quote", "\"").option("escape", "\"").csv(new_files)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = prevent_duplicate_data(df)

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
    write_method=write_method,
    schema_evolution=True,
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
