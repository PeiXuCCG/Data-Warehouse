# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

# %%
from pyspark.sql import SparkSession
from loom.tables.keyed_table import KeyedTable
from loom.pipelines import Pipeline
from pyspark.sql.functions import col, sha2
from schemabridge4bc.schemabridge.bridgeschemas import transform_using_schema_bridge


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
target_table = "historical_customer"

# source tables
source_schema = "raw"
source_table = "netsuite_customer"
source_system = "Netsuite"

# source keys 
source_primary_keys = ["customerid"]
source_foreign_keys = ["location"] # fill these in if there are any
business_keys = ["location"] # this is used for partition the table in the lakehouse, helpful for querying

# fields to build primary key on
deduplicate_fields = ["customer_name", "phone", "address"] # please change this for each entity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform(df):
    # import mapping library here and convert to bc schema
    df = transform_using_schema_bridge(source_system, target_table.split("_")[1], df)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def deduplicate(df):
    # This doesn't change the referential integrity of the data.
    # We are assigning a primary key to all the records that match based on the selected fields.
    # Essentially, for records that have identical values in these fields, we generate the same primary key.
    # This is useful for deduplication, grouping, or creating a consistent identifier without modifying
    # the relationships between tables or violating foreign key constraints.
    df = (
            df.withColumn(
                f"{target_table}_hk",
                sha2(concat_ws("|", *[col(c) for c in deduplicate_fields]), 256)
            )
    )       
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
df = spark.read.table(f"{source_schema}.{source_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%
# 4. Create the curated keyed table
bronze_table  = KeyedTable(
            target_db=target_db,
            target_schema=target_schema,
            name=target_table,
            schema_evolution=False,
            df=df,
            target_path="", # NOT SUPPORTED IN FABRIC
            business_keys=business_keys,
            source_primary_keys=source_primary_keys,
            source_foreign_keys=source_foreign_keys,
            transform=transform, # This is used for converting the schema from Historical to BC schema using schemabridge4BC
            deduplicate=deduplicate # this creates the primary key for the table
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
