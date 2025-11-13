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
from pyspark.sql import SparkSession
from loom.tables.keyed_table import KeyedTable
from loom.pipelines import Pipeline
from pyspark.sql.functions import col, sha2, window, current_timestamp, lag


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# %%
# These are the input variables for each bronze table
target_schema = "lh_bronze_dev"
target_db = "bronze"
target_table = "customer"

# source - don't need source schema
source_db = "raw"
source_table = "customer"

# source keys 
source_primary_keys = ["no"]
source_foreign_keys = ["locationcode"] # fill these in if there are any
business_keys = [] # this is used for partition the table in the lakehouse, helpful for querying but isn't required

# how to build the primary key
deduplicate_fields = ["name", "phoneno", "address"] # please change this for each entity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform(df):
    # NOT USED IN BC implementation
    pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
def deduplicate(df):
    # This doesn't change the referential integrity of the data.
    # We are assigning a primary key to all the records that match based on the selected fields.
    # Essentially, for records that have identical values in these fields, we generate the same primary key.
    # This is useful for deduplication, grouping, or creating a consistent identifier without modifying
    # the relationships between tables or violating foreign key constraints.
    # But we do want to end date those duplicates
    df = (
            df.withColumn(
                    f"{target_table}_hk",
                    sha2(concat_ws("|", *[col(c) for c in deduplicate_fields]), 256)
            )
    ) 

    # this is metadata that we need to override that was in the loom framework (default to 1900)
    df = df.withColumn("effectivity_start_date", lit("1900-01-01 00:00:00").cast("timestamp"))

    # Define window partitioned by hash key, ordered by lastdatemodified descending (this is a field in BC)
    w = window.partitionBy(f"{target_table}_hk").orderBy(F.col("lastdatemodified").desc())

    # Use lag to get the next record’s lastdatemodified (in descending order)
    df = df.withColumn(
        "effectivity_end_date",
        lag("lastdatemodified").over(w).cast("timestamp")
    )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
df = spark.read.table(f"{source_db}.{source_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# 4. Create the curated keyed table
bronze_table = KeyedTable(
            target_db=target_db,
            target_schema=target_schema,
            name=target_table,
            schema_evolution=False,
            df=df,
            target_path="", # NOT SUPPORTED IN FABRIC
            business_keys=business_keys,
            source_primary_keys=source_primary_keys,
            source_foreign_keys=source_foreign_keys,
            transform=transform, # YOU CAN CHANGE THIS TO NONE if there is transformations to be completed.
            deduplicate=deduplicate # this generates the primary key for the table based off the fields you provide
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
    tables=bronze_table,
    dry_run=True,   # simulate execution without writing
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
