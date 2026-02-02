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
from notebookutils import mssparkutils
from loom.tables.keyed_table import KeyedTable
from loom.pipelines import Pipeline
from pyspark.sql.functions import to_json, create_map,map_filter, sum, monotonically_increasing_id, coalesce,lower, expr,regexp_replace, col, sha2, window, current_timestamp, lag, concat_ws, lit, row_number, when
from pyspark.sql import Window
from schemabridge4bc.schemabridge.bridgeschemas import transform_using_schema_bridge
import re
import json


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
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

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
target_table = "contract_customer"

# source tables
source_lakehouse="lh_bronze"
source_schema = "raw"
source_table = "cuontracts_customer"
source_system = "Contracts"

skip_activities = []
activity = "Customer"


# source keys (in bc form, not source field names)
source_key = "no" # this is used in the schemabridge for capture the unmapped columns
source_primary_keys = "[\"no\", \"company\", \"source_system\"]"
source_foreign_keys = """[{\"city\": [\"city\", \"source_system\", \"company\"]},
                        {\"postcode\": [\"postcode\", \"source_system\", \"company\"]},
                        {\"county\": [\"county\", \"source_system\", \"company\"]} , 
                        {\"customerpostinggroup\":[\"customerpostinggroup\", \"source_system\", \"company\"]}
                      ]""" # only the ones that are mapped will work here
business_keys = "[\"locationcode\"]" # this is used for partition the table in the lakehouse, helpful for querying

# fields to build primary key on
deduplicate_fields = "[\"phoneno\", \"address\", \"mobilephoneno\"]" # please change this for each entity

dry_run = True # need to override this to make it save to the schema



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

workspace_name = mssparkutils.env.getWorkspaceName()

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
         df = df.withColumn(
            f"{target_table}_hk",
            sha2(
                concat_ws(
                    "|",
                    *[
                        regexp_replace(
                            lower(coalesce(col(c), lit(""))), "\\s+", ""
                        )
                        for c in source_primary_keys
                    ]
                ),
                256
            )
        )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_func(df):
   
   
    # Transform
    CAMEL_CASE_REGEX = re.compile(r'^[a-z]+[A-Z][a-zA-Z0-9]*$')

    def is_camel_case(col_name: str) -> bool:
        return bool(CAMEL_CASE_REGEX.match(col_name))

    camel_case_cols = [c for c in df.columns if is_camel_case(c)]

    json_expr = to_json(
        map_filter(
            create_map(
                *[x for c in camel_case_cols for x in (lit(c), col(c))]
            ),lambda k, v: v.isNotNull()
        )
    )

    df_with_unmapped = df.withColumn("unmapped", json_expr)

    return df_with_unmapped

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # %%
# # %%
# path = f"abfss://f368bfab-68d5-4371-9d51-086e4d741baf@onelake.dfs.fabric.microsoft.com/a339ad8e-4b05-473e-9858-4555a6d87f3d/Tables/dbo/{source_table}"

# if mssparkutils.fs.exists(path):
#     df = spark.read.format("delta").load(path)
# else:
#    mssparkutils.notebook.exit(f"{path} doesn't exist")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(f"{source_schema}.{source_table}"):
    df = spark.read.table(f"{source_schema}.{source_table}")
else:
   mssparkutils.notebook.exit(f"{source_table} doesn't exist in raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# 4. Create the curated keyed table
bronze_table  = KeyedTable(
            target_db=target_schema,
            target_schema=target_lakehouse,
            name=target_table,
            schema_evolution=True,
            df=df,
            target_path="NOT_SUPPORTED_YET", # NOT SUPPORTED IN FABRIC
            business_keys=business_keys,
            source_primary_keys=source_primary_keys,
            source_foreign_keys=source_foreign_keys,
            transform=transform_func, # This is used for converting the schema from Historical to BC schema using schemabridge4BC
            deduplicate=deduplicate_func # this creates the primary key for the table
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
