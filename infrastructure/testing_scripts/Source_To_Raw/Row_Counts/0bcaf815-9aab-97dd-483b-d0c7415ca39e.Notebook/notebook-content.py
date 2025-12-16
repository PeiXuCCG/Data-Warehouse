# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b66c596f-0807-41d8-b506-c2b21fcb9f29",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "ac4f6d6f-0d6d-4c24-b56e-a49baf8c7706",
# META       "known_lakehouses": [
# META         {
# META           "id": "b66c596f-0807-41d8-b506-c2b21fcb9f29"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "39f1badd-f616-482b-9dc2-e9db7c8b2617",
# META       "workspaceId": "ac4f6d6f-0d6d-4c24-b56e-a49baf8c7706"
# META     }
# META   }
# META }

# CELL ********************

business_type = "CCG" #KCARE

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# You need to navigate to page=8700 and download the excel file. as save it as a CSV into the paths below

ccg_file = "/lakehouse/default/Files/testing/CCG/Table Information.csv" # https://businesscentral.dynamics.com/2e91d91e-f10a-4460-a8f0-37a2e9ef9221/UAT?page=8700
kcare_file = "/lakehouse/default/Files/testing/KCare/Table Information.csv" # https://businesscentral.dynamics.com/2e91d91e-f10a-4460-a8f0-37a2e9ef9221/KCAREUAT?page=8700

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.functions import * 
import pandas as pd
import re
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_system = "BC"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if businesstype == "CCG":
  company = [
    ("Retail Country Care Group","bc"),
    ("K Care Holdings Pty Ltd","bc"),
    ("CCG Contracts","bc"),
    ("Ind. Healthcare Solutions","bc"),
    ("Management Country Care Group","bc"),
    ("QLD Rehab Equipment","bc"),
    ("CCGA Holdings","bc"),
    ("CCGA Bidco Pty Ltd", "bc")
  ]
  bc_count_file = ccg_file
else:
  company = [
    ("K Care Healthcare Solutions","bc"),
  ]
  bc_count_file = kcare_file


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

entities = []

tables = spark.sql("SHOW TABLES IN lh_bronze.raw") \
              .toPandas()


for t in tables['tableName'].tolist():
    if source_system.lower() in  t:
        entities.append(t.split("bc_")[1])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = []

for name, system in company:   # your original "for name, system in company:"
    print(f"------------ Starting review of {name} ------------------")


    for e in entities:
            table = f"raw.{system}_{e}"

            if spark.catalog.tableExists(table):
                df = spark.sql(f"""
                    SELECT COUNT(*) as cnt
                    FROM {table}
                    WHERE company = '{name}'
                """)

                count_val = df.collect()[0]["cnt"]
                results.append(Row(entity=e, company=name, count=count_val))

            else:
                print(f"{system}_{e} doesn't exist")
                results.append(Row(entity=e, company=name, count=None))

    print(f"--------------- Ending review of {name} ------------------")


# Convert to Spark DataFrame
results_df = spark.createDataFrame(results)

# Pivot into desired layout
pivot_df = (
    results_df
        .groupBy("entity")
        .pivot("company")
        .sum("count")
        .orderBy("entity")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def cleanse(df):
    pattern = r"[^a-z0-9_]+"

    new_cols = {}
    for col in df.columns:
        cleaned = re.sub(pattern, "", col.split("-")[0].lower())
        new_cols[col] = cleaned

    df_cleaned = df.rename(columns=new_cols)

    return df_cleaned


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load data into pandas DataFrame from "/lakehouse/default/Files/testing/Table Information.csv"
df = pd.read_csv("/lakehouse/default/Files/testing/Table Information.csv")
rows = []
for name, system in company:
    filtered_df = df[df["Company Name"].str.lower() == name.lower()]
    filtered_df = cleanse(filtered_df)
    for e in entities:
        # Get the row for that entity
        row = filtered_df[filtered_df["tablename"].str.lower().str.replace(r'[^a-z0-9]', '', regex=True) == e.lower()]

        value = row["noofrecords"].iloc[0] if not row.empty else None

        rows.append({
            "entity": e, 
            "company": name,
            "value": str(value) if value is not None else None
        })


schema = StructType([
    StructField("entity", StringType(), True),
    StructField("company", StringType(), True),
    StructField("value", StringType(), True)   # Spark LongType == pandas int64
])

spark_df = spark.createDataFrame(rows, schema=schema)


pivot_df2 = (
    spark_df
        .groupBy("entity")
        .pivot("company")
        .agg(first("value"))
)

# Rename to BC_<company>
for name, system in company:
    pivot_df2 = pivot_df2.withColumnRenamed(name, f"BC_{name}")


columns_to_select = ["entity"]
for name, system in company:
    columns_to_select.append(f"`{name}`")
    columns_to_select.append(f"`BC_{name}`")

display(pivot_df2.join(pivot_df, "entity", "outer").select(columns_to_select))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
