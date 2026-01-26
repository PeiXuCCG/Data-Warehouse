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

# %%
from pyspark.sql import SparkSession, Row
from loom.tables.keyed_table import KeyedTable
from loom.pipelines import Pipeline
from pyspark.sql.functions import to_date, count, trim, first, from_unixtime, monotonically_increasing_id, coalesce,lower, expr,regexp_replace, col, sha2, window, current_timestamp, lag, concat_ws, lit, row_number, when, sum
from pyspark.sql import Window
from schemabridge4bc.schemabridge.bridgeschemas import transform_using_schema_bridge
import re
import json
from notebookutils import mssparkutils
from pyspark.sql.types import DecimalType, IntegerType, DateType, TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")  # optional, for parsing strings


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# %%
# These are the input variables for each bronze table
target_lakehouse = "lh_bronze"
target_schema = "bronze"
target_table = "glentry"

# source - don't need source schema
source_lakehouse = "lh_bronze"
source_schema = "raw"
source_table = "bc_glentry"

skip_activities = []
activity = "GLEntry"


# source keys 
source_primary_keys = ["documentno", "description", "company", "source_system"]
source_foreign_keys = [
                        {"glaccountno": ["glaccountno", "source_system", "company"]},
                        {"documenttype": ["documenttype", "source_system", "company"]},
                        {"balaccountno": ["balaccountno", "source_system", "company"]},
                        {"sourcecode": ["sourcecode", "source_system", "company"]},
                        {"jobno": ["jobno", "source_system", "company"]},
                        {"reasoncode": ["reasoncode", "source_system", "company"]},
                        {"genbuspostinggroup": ["genbuspostinggroup", "source_system", "company"]},
                        {"genprodpostinggroup": ["genprodpostinggroup", "source_system", "company"]},
                        {"balaccounttype": ["balaccounttype", "source_system", "company"]},
                        {"sourcetype": ["sourcetype", "source_system", "company"]},
                        {"sourceno": ["sourceno", "source_system", "company"]},
                        {"taxareacode": ["taxareacode", "source_system", "company"]},
                        {"taxgroupcode": ["taxgroupcode", "source_system", "company"]},
                        {"vatbuspostinggroup": ["vatbuspostinggroup", "source_system", "company"]},
                        {"vatprodpostinggroup": ["vatprodpostinggroup", "source_system", "company"]},
                        {"allocationaccountno": ["allocationaccountno", "source_system", "company"]},
                        {"prodorderno": ["prodorderno", "source_system", "company"]}
                      ] # fill these in if there are any
business_keys = ["originating_company",  "postingdate"] # this is used for partition the table in the lakehouse, helpful for querying but isn't required

# how to build the primary key
deduplicate_fields = [] # please change this for each entity

dry_run = True # you need to override this to false to make it save to the schema

workspace_name = mssparkutils.env.getWorkspaceName()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pipeline_name = f"{source_schema}_{source_table}_to_{target_schema}_{target_table}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

decimal_fields = ["amount", "quantity", "qty"]
integer_fields = []
date_fields = ["postingdate", "duedate", "startingdate"]
timestamp_fields = ["effectivity_start_date", "effectivity_end_date"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reload strings as arrays
deduplicate_fields = json.loads(deduplicate_fields)
business_keys = json.loads(business_keys)
source_primary_keys = json.loads(source_primary_keys)
source_foreign_keys = json.loads(source_foreign_keys)
skip_activities = json.loads(skip_activities)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if activity in skip_activities:
    mssparkutils.notebook.exit(f"Skipping activity {activity}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_func(df):
    # NOT USED IN BC implementation
    pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def dedupe_columns(df):
    seen = {}
    new_cols = []
    for c in df.columns:
        if c in seen:
            seen[c] += 1
            new_cols.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            new_cols.append(c)
    return df.toDF(*new_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def deduplicate_func(df):

    if len(deduplicate_fields) > 0:

        # --- Generate HK hash key ---
        df = df.withColumn(
            f"{target_table}_hk",
            sha2(
                concat_ws(
                    "|",
                    *[
                        regexp_replace(
                            lower(coalesce(col(c), lit(""))), "\\s+", ""
                        )
                        for c in deduplicate_fields
                    ]
                ),
                256
            )
        )

        # --- Fixed effectivity_start_date ---
        df = df.withColumn(
            "effectivity_start_date",
            lit("1900-01-01 00:00:00").cast("timestamp")
        )

        # --- Pure surrogate ordering across duplicates ---
        df = df.withColumn(
            "surrogate_order",
            monotonically_increasing_id()
        )

        # Window by hash key ordered **only** by surrogate_order (descending → newest first)
        w = (
            Window
            .partitionBy(f"{target_table}_hk")
            .orderBy(col("surrogate_order").desc())
        )

        # --- SCD2 chaining using surrogate ID ---
        now_ts = current_timestamp()

        df = (
            df.withColumn("rn", row_number().over(w))
            .withColumn(
                "effectivity_end_date",
                when(col("rn") == 1, lit(None))   # newest version → still open
                .otherwise(now_ts)               # older versions → closed now
            )
            .drop("rn", "surrogate_order")
        )
    else:
         #safety first
         df = dedupe_columns(df)
         # hash all columns for the primary key
         df = df.withColumn(
            f"{target_table}_hk",
            sha2(concat_ws("||", *df.columns), 256)
        ).withColumn("effectivity_end_date", lit(None).cast(TimestampType())).withColumn("effectivity_start_date", current_timestamp())

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cast_fields_to_decimal(df, fields, precision=18, scale=2):
    """
    Cast specified fields to DecimalType if they exist in the DataFrame.
    
    :param df: Input DataFrame
    :param fields: List of field names to cast
    :param precision: Decimal precision
    :param scale: Decimal scale
    :return: DataFrame with specified fields cast to DecimalType
    """
    transformed_cols = []
    
    for c in df.columns:
        if c in fields:
            transformed_cols.append(trim(col(c)).cast(DecimalType(precision, scale)).alias(c))
        else:
            transformed_cols.append(col(c))
    
    return df.select(*transformed_cols)
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cast_fields_to_integer(df, fields):
    """
    Cast specified fields to IntegerType if they exist in the DataFrame.
    
    :param df: Input DataFrame
    :param fields: List of field names to cast
    :return: DataFrame with specified fields cast to DecimalType
    """
    transformed_cols = []
    
    for c in df.columns:
        if c in fields:
            transformed_cols.append(trim(col(c)).cast(IntegerType()).alias(c))
        else:
            transformed_cols.append(col(c))
    
    return df.select(*transformed_cols)
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cast_fields_to_date(df, fields):
    """
    Cast specified fields to IntegerType if they exist in the DataFrame.
    
    :param df: Input DataFrame
    :param fields: List of field names to cast
    :return: DataFrame with specified fields cast to DecimalType
    """
    transformed_cols = []
    
    for c in df.columns:
        if c in fields:
            transformed_cols.append(trim(col(c)).cast(DateType()).alias(c))
        else:
            transformed_cols.append(col(c))
    
    return df.select(*transformed_cols)
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def cast_fields_to_timestamp(df, fields):
    """
    Cast specified fields to TimestampType if they exist in the DataFrame.
    
    :param df: Input DataFrame
    :param fields: List of field names to cast
    :return: DataFrame with specified fields cast to DecimalType
    """
    transformed_cols = []
    
    for c in df.columns:
        if c in fields:
            transformed_cols.append(trim(col(c)).cast(TimestampType()).alias(c))
        else:
            transformed_cols.append(col(c))
    
    return df.select(*transformed_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
if spark.catalog.tableExists(f"{source_lakehouse}.{source_schema}.{source_table}"):
    df = spark.read.table(f"{source_lakehouse}.{source_schema}.{source_table}")
else:
   mssparkutils.notebook.exit(f"{source_table} doesn't exist in raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = cast_fields_to_decimal(df, decimal_fields)
df = cast_fields_to_integer(df, integer_fields)
df = cast_fields_to_date(df, date_fields)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# 4. Create the curated keyed table
bronze_table = KeyedTable(
            target_db=target_schema,
            target_schema=target_lakehouse,
            name=target_table,
            schema_evolution=True,
            df=df,
            target_path="NOT_SUPPORTED_YET", # NOT SUPPORTED IN FABRIC
            business_keys=business_keys,
            source_primary_keys=source_primary_keys,
            source_foreign_keys=source_foreign_keys,
            transform=None, # YOU CAN CHANGE THIS TO NONE if there is transformations to be completed.
            deduplicate=deduplicate_func # this generates the primary key for the table based off the fields you provide
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# 5. Build the pipeline
pipeline = Pipeline(
    name=pipeline_name,
    tables=[bronze_table],
    dry_run=dry_run,   # simulate execution without writing
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# 6. Execute the pipeline
pipeline.summary()   # Prints info about each table
pipeline.validate()  # Validates structure and metadata
pipeline.execute()   # Runs the prepare + write steps

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
