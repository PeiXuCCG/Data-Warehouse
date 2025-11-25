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
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import col, to_date, current_timestamp

def load_rules_from_excel(excel_path: str
                        ,table_name: str = "dbo.cleaning_rules_set"
                        ,created_by_user: str = "unknown"):

    """
    Load data cleaning rules from an Excel file into a Fabric Delta table.

    Parameters:
        excel_path (str): Path inside the Lakehouse Files area.
        table_name (str): Target Delta table to write into.
        created_by_user (str): Who uploaded/loaded the file.
    """
    
    # reading the excel file
    df_raw = (
        spark.read.format("excel")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(excel_path)
    )

    #Transform Schema

    df_rules = (
        df_raw
        .withColumn("source_columns", F.split(col("source_columns"), ","))
        .withColumn("rule_set", col("rule_set").cast(StringType()))
        .withColumn("effective_starts_date", to_date(col("effective_start_date"), "yyyy-MM-dd"))
        .withColumn("effective_end_date",to_date(col("effective_end_date"), "yyyy-MM-dd"))
        .withColumn("created_by", F.lit(created_by_user))
        .withColumn("created_date", current_timestamp())
    )

    # Making sure 

    spark.sql("""
    create table if not EXISTS {table_name}(
        id BIGINT,
        source_column ARRAY<STRING>,
        target_column STRING,
        rule_set STRING,
        cleaning_rule STRING,
        effectivity_start_date DATE,
        effectivity_end_date DATE,
        create_by STRING,
        create_date TIMESTAMP
    )

    USING DELTA
    
    """)


    df_rules.write.format('delta').mode('append').saveAsTable(table_name)

    print('Successfull loaded rules from {excel_path} into {table_name}')




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
