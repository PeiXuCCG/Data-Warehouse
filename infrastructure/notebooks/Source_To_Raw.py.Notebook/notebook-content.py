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
from pyspark.sql.functions import broadcast, lit, concat_ws, col, sha2, input_file_name, first
import re
from notebookutils import mssparkutils
import sys
import os
from pyspark.sql.types import IntegerType, DoubleType, DateType, StringType
import datetime


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
# conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
# conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
# conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")


# CELL ********************

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

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
target_table = f"bc_glentry"
source_path = 'Files/deltas/GLEntry-17'
is_multi_line = True
write_method = "overwrite"
infer_schema = False
dry_run=False
workspace_name = mssparkutils.env.getWorkspaceName()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

        if '$' in base and "company" not in base:
            #set the prefix to bc2adls for deliverytime 
            base = f"bc2adls_{base}"

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
        loaded_files = [r["source_file"].split("?")[0] for r in log_df.collect()]

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

def add_row_hash(df, cols):
    concat_cols = concat_ws(
        "||",
        *[col(f"`{c}`").cast("string") for c in cols]
    )
    return df.withColumn("row_hash", sha2(concat_cols, 256))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def prevent_duplicate_data(df, already_existing_df):
    exclude_cols = [
        "source_system", "source_file", "systemmodifiedat", "systemmodifiedby",
        "delivereddatetime", "ingestion_timestamp", "batch_id",
        "bc2adls_delivereddatetime", "systemid"
    ]

    if already_existing_df.isEmpty():
        df_hashed = add_row_hash(
            df,
            sorted(set(df.columns) - set(exclude_cols))
        )
        return df_hashed.dropDuplicates(["row_hash"]).drop("row_hash")

    common_cols = sorted(
        set(df.columns) - set(exclude_cols)
        & set(already_existing_df.columns)
    )

    df_hashed = add_row_hash(df, common_cols)
    existing_hashed = add_row_hash(already_existing_df, common_cols)

    result = df_hashed.join(
        broadcast(existing_hashed.select("row_hash")),
        on="row_hash",
        how="left_anti"
    ).drop("row_hash")

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_col(col_name):
    """
    Clean column names:
      - lowercases
      - replaces non-alphanumeric chars
    """
    col_name = col_name.lower()
    col_name = re.sub(r'[^a-zA-Z0-9]', '', col_name)
    return col_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def align_headers(df, master_columns):
    """
    Return a DataFrame with columns aligned to master_columns **by name**:
      - keeps exact existing columns (by cleaned name)
      - inserts NULL columns for missing master columns
      - does NOT keep any extra columns not in master_columns
      - prevents positional misalignment by building explicit select expressions

    master_columns: iterable of column-names (strings).
    """
    # 1) Clean master column names and ensure they are unique (preserve order)
    cleaned_master = []
    seen = set()
    for c in master_columns:
        cn = clean_col(c)
        if cn in seen:
            # if duplicate in master, skip duplicate 
            continue
        seen.add(cn)
        cleaned_master.append(cn)

    # 2) Map existing df columns by cleaned name -> actual name (preserve first occurrence)
    df_cols = df.columns
    df_map = {}
    for actual in df_cols:
        key = clean_col(actual)
        # if duplicates in source, keep the first mapping; duplicates should be careful
        if key not in df_map:
            df_map[key] = actual

    # 3) Build select expressions explicitly
    exprs = []
    for m in cleaned_master:
        if m in df_map:
            exprs.append(col(df_map[m]).alias(m))
        else:
            exprs.append(lit(None).alias(m))

    # 4) Select in the master order
    return df.select(*exprs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

    # ---- 1. Deduplicate df column names ----
    def dedupe_columns(cols):
        seen = {}
        new_cols = []
        for c in cols:
            if c not in seen:
                seen[c] = 1
                new_cols.append(c)
            else:
                seen[c] += 1
                new_name = f"{c}_{seen[c]}"
                new_cols.append(new_name)
        return new_cols

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def align_headers_dynamic(df, master_columns):
    """
    Aligns df columns to master_columns:
    - Adds missing columns as NULL
    - Reorders columns to match master_columns
    - Appends new columns from df to master_columns
    - If duplicate column names appear, renames duplicates:
        col, col_2, col_3, etc.
    """



    original_cols = df.columns
    deduped_cols = dedupe_columns(original_cols)

    # Rename df columns if needed
    for old, new in zip(original_cols, deduped_cols):
        if old != new:
            df = df.withColumnRenamed(old, new)

    df_cols = deduped_cols

    # ---- 2. Deduplicate master columns too ----
    master_columns = dedupe_columns(master_columns)

    new_master_cols = master_columns.copy()

    # ---- 3. Add df columns to master if missing ----
    for col in df_cols:
        if col not in new_master_cols:
            new_master_cols.append(col)

    # ---- 4. Add missing df columns as NULL ----
    for col in new_master_cols:
        if col not in df_cols:
            df = df.withColumn(col, lit(None))

    
    # ---- 5. Rename duplicate columns from source dataframe ----
    new_cols = []
    counts = {}

    for i, c in enumerate(df.columns):
        counts[c] = counts.get(c, 0) + 1
        alias = c if counts[c] == 1 else f"{c}_{counts[c]}"
        new_cols.append(col(df.columns[i]).alias(alias))

    df = df.select(*new_cols)

    # ---- 6. Reorder to match master ----
    df = df.select(list(dict.fromkeys(new_master_cols)))

    return df, list(dict.fromkeys(new_master_cols))

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
            .option("quote", '"') 
            .option("escape", '\\') 
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(file)
    )

    if source_system != "BC":
        # Historical sources: clean columns
        cleaned_cols = [clean_col(c) for c in one_df.columns]
        one_df = one_df.toDF(*cleaned_cols)

        # call cleanse engine here
        one_df = clean(one_df, spark.table(rules_table), source_system)

        # First file defines master schema
        if df is None:
            df = one_df
            master_columns = df.columns
            print(f"Initial {source_system} schema", master_columns)
            continue

        # Align columns to master schema
        one_df = align_headers(one_df, master_columns)
        df = df.unionByName(one_df)

    else:
        # ---- BC logic: dynamic schema ----
        # BC sources
        cleaned_df = cleanse(one_df)

        if df is None:
            df = cleaned_df
            master_columns = dedupe_columns(df.columns)
            print("Initial BC master schema:", master_columns)
            continue

        # Align df and update master schema dynamically
        cleaned_df, master_columns = align_headers_dynamic(cleaned_df, master_columns)

        # Also align the existing df in case new columns appeared
        df, master_columns = align_headers_dynamic(df, master_columns)

        df = df.unionByName(cleaned_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn("_source_file_temp", input_file_name())

# Add a column so it survives renames/select/etc
df = df.withColumn("source_file", col("_source_file_temp")).drop("_source_file_temp")

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

already_existing_df = spark.createDataFrame([], df.schema)

if spark.catalog.tableExists(f"{target_schema}.{target_db}.{target_table}"):
    already_existing_df = spark.read.table(f"{target_schema}.{target_db}.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = prevent_duplicate_data(df, already_existing_df)

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
    cleanse_function=None #Doing the cleanse earlier now
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
