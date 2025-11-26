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

from loom.tables.table_type  import TableType
from loom.tables.plain_table import PlainTable
from loom.pipelines import Pipeline
from data_cleaning_rules.rule_engine import clean
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, concat_ws, col, sha2, input_file_name, first
import re
from notebookutils import mssparkutils
import sys
import os


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
infer_schema = False
dry_run=False

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
rules_table = "dbo.cleaning_rules_set"
pipelines = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cleanse(df):
    pattern = r"[^a-z0-9_]+"
    rename_map = {}
    drop_cols = []

    for col in df.columns:
        # Step 1: split on '-' and lowercase
        base = col.split("-")[0].lower()

        # Step 2: remove special chars
        sanitized = re.sub(pattern, "", base)

        # Step 3: Skip predictionconfidence
        if sanitized == "predictionconfidence":
            drop_cols.append(col)
            continue

        rename_map[col] = sanitized

    # Apply renames safely
    df_cleaned = df
    for original, new in rename_map.items():
        if original != new:
            df_cleaned = df_cleaned.withColumnRenamed(original, new)

    # Drop unwanted columns
    if drop_cols:
        df_cleaned = df_cleaned.drop(*drop_cols)

    # call cleanse engine here
    df_cleaned = clean(df_cleaned, spark.table(rules_table), source_system)


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
        print("Ingestion Log Table loaded successfully.")
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

def align_headers(df, master_columns):
    """
    Adds missing columns as NULL.
    Drops extra columns.
    Reorders columns to match master_columns.
    """

    df_cols = df.columns

    # Add missing columns
    for col in master_columns:
        if col not in df_cols:
            df = df.withColumn(col, F.lit(None))

    # Drop unexpected columns
    for col in df_cols:
        if col not in master_columns:
            df = df.drop(col)

    # Reorder to match master schema
    df = df.select(master_columns)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

master_columns = None
df = None

for file in new_files:
    if not file.lower().endswith(".csv"):
        continue



    print(f"Loading {file}")

    one_df = (
        spark.read
            .option("header", True)
            .option("inferSchema", infer_schema)
            .option("multiLine", True)
            .option("quote", "\"")
            .option("escape", "\"")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(file)
    )

    # First file defines the master schema
    if df is None:
        df = one_df
        master_columns = df.columns
        continue

    one_df = align_headers(one_df, master_columns)
    df = df.unionByName(one_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_file = df.withColumn("_source_file_temp", input_file_name())

# Collect a single filename (in case Spark splits files)
filename = df_with_file.select(first("_source_file_temp", ignorenulls=True)).collect()[0][0]

# Add a literal column so it survives renames/select/etc
df = df.withColumn("source_file", lit(filename))

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

df = prevent_duplicate_data(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw = PlainTable(
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
    tables=[raw],
    dry_run=dry_run,  # Set to True to simulate without writing
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
