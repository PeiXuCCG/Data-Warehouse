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
from pyspark.sql.functions import from_unixtime, monotonically_increasing_id, coalesce,lower, expr,regexp_replace, col, sha2, window, current_timestamp, lag, concat_ws, lit, row_number, when, sum
from pyspark.sql import Window
from schemabridge4bc.schemabridge.bridgeschemas import transform_using_schema_bridge
import re
import json

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
target_table = "customer"

# source - don't need source schema
source_lakehouse = "lh_bronze"
source_schema = "raw"
source_table = "bc_customer"

pipeline_name = f"{source_schema}_{source_table}_to_{target_schema}_{target_table}"

# source keys 
source_primary_keys = ["no", "source_system", "company"]
source_foreign_keys = [
                        {"city": ["city", "source_system", "company"]},
                        {"postcode": ["postcode", "source_system", "company"]},
                        {"county": ["county", "source_system", "company"]} , 
                        {"customerpostinggroup":["customerpostinggroup", "source_system", "company"]},
                        {"customerpricegroup":["customerpricegroup", "source_system", "company"]},
                        {"locationcode": ["locationcode", "source_system", "company"]},
                        {"shiptocode": ["shiptocode", "source_system", "company"]},
                        {"globaldimension1code": ["globaldimension1code", "source_system", "company"]},
                        {"globaldimension2code": ["globaldimension2code", "source_system", "company"]},
                        {"languagecode": ["languagecode", "source_system", "company"]},
                        {"paymenttermscode": ["paymenttermscode", "source_system", "company"]},
                        {"customerdiscgroup": ["customerdiscgroup", "source_system", "company"]},
                        {"countryregioncode": ["countryregioncode", "source_system", "company"]},
                        {"territorycode": ["territorycode", "source_system", "company"]},
                        {"salespersoncode": ["salespersoncode", "source_system", "company"]},
                        {"shipmentmethodcode": ["shipmentmethodcode", "source_system", "company"]},
                        {"shippingagentcode": ["shippingagentcode", "source_system", "company"]},
                        {"billtocustomerno": ["billtocustomerno", "source_system", "company"]},
                        {"paymentmethodcode": ["paymentmethodcode", "source_system", "company"]}
                      ] # fill these in if there are any
business_keys = ["locationcode"] # this is used for partition the table in the lakehouse, helpful for querying but isn't required

# how to build the primary key
deduplicate_fields = ["phoneno", "address", "mobilephoneno"] # please change this for each entity

dry_run = True # you need to override this to false to make it save to the schema

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

def deduplicate_func(df):

    # --- Build unified lastdatemodified using available columns ---
    lastmod_candidates = [
        "systemmodifiedat",
        "lastmodifieddatetime",
        "lastdatemodified",
        "timestamp"
    ]

    # Pick only columns that exist in df
    available_cols = [c for c in lastmod_candidates if c in df.columns]

    if not available_cols:
        # If none exist, create a NULL column
        df = df.withColumn("lastdatemodified", lit(None).cast("timestamp"))
    else:
        df = df.withColumn(
            "lastdatemodified",
            coalesce(*[col(c).cast("timestamp") for c in available_cols])
        )

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

    # Start date always 1900
    df = df.withColumn(
        "effectivity_start_date",
        lit("1900-01-01 00:00:00").cast("timestamp")
    )

    # Surrogate ordering for NULL lastdatemodified groups
    df = df.withColumn("surrogate_order", monotonically_increasing_id())

    df = df.withColumn("has_lastmod", col("lastdatemodified").isNotNull().cast("int"))

    # Window by hash key
    w_grp = Window.partitionBy(f"{target_table}_hk")

    df = df.withColumn(
        "ordering_key",
        when(
            sum("has_lastmod").over(w_grp) == lit(0),     # all NULL
            col("surrogate_order")                   # deterministic fallback
        ).otherwise(
            col("lastdatemodified").cast("long")
        )
    )

    # Sort newest first
    w = Window.partitionBy(f"{target_table}_hk").orderBy(col("ordering_key").desc())

    df = (
        df.withColumn(
            "effectivity_end_date",
            lag("ordering_key").over(w).cast("timestamp")
        )
        .withColumn("rn", row_number().over(w))
        .withColumn(
            "effectivity_end_date",
            when(col("rn") == 1, lit(None)).otherwise(col("effectivity_end_date"))
        )
        .drop("rn", "surrogate_order", "has_lastmod", "ordering_key")
    )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
df = spark.read.table(f"{source_lakehouse}.{source_schema}.{source_table}")

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
            schema_evolution=False,
            df=df,
            target_path="", # NOT SUPPORTED IN FABRIC
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

try:
    spark.stop()
except:
    pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
