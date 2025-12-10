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

from pyspark.sql import Row
from pyspark.sql.functions import concat_ws, col, sha2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Don't change these
companies = [
    "vital_living",
    "merits"
]

schema_name = "raw"
system = "netsuite"

history_path = "Files/history/"
updated_tables = []


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def prevent_duplicate_data(df):
    # This is a BC thing, where by it will BC2ADLS will extract a new file 
    # everytime there is a change in the schema, this can result 
    # duplicate data being extracted
    concat_cols = concat_ws("||", *[col(c).cast("string") for c in df.columns if c != 'delivereddatetime'])
    df_hashed = df.withColumn("row_hash", sha2(concat_cols, 256))

    df_deduped = df_hashed.dropDuplicates(["row_hash"]).drop("row_hash")
    return df_deduped

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def contains_csvs(path):
    try:
        files =  mssparkutils.fs.ls(path)
        return any(f.name.endswith(".csv") for f in files)
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

# Retrieve record counts for each company on each table 
results = []
tables_df = spark.sql(f"SHOW TABLES IN lh_bronze.{schema_name}")

tables = tables_df.collect()

for t in tables:
    table_name = t.tableName
    full_name = f"lh_bronze.{schema_name}.{table_name}"

    if system in table_name:
        print(table_name)
        for name in companies:
          df = spark.sql(f"""
                    SELECT COUNT(*) as cnt
                    FROM {full_name}
                    WHERE company = '{name}'
                """)

          count_val = df.collect()[0]["cnt"]
          results.append(Row(entity=table_name, company=name, table_count=count_val, file_count=0))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t in tables:
    if system in t.tableName:
        print(t.tableName)
        entity = t.tableName.split("xero_")[1]
        for c in companies:
            company_path = '_'.join(c.split(" ")).lower()
            full_path =f"{history_path}/{system}/{company_path}/{entity}"
            if contains_csvs(full_path):
                print(f"checking CSVs under {full_path}")
                df = prevent_duplicate_data(spark.read.option("header", True).option("multiLine", True).option("quote", "\"").option("escape", "\"").csv(full_path))
                count_value = df.count()
            
                for r in results:
                    if r["entity"] == t.tableName and r['company'] == c:
                        print("adding to results")
                        t_dict = r.asDict()
                        t_dict["file_count"] = count_value

                        # create a new Row with updated attributes
                        updated_tables.append(Row(**t_dict))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_passed = True
for u in updated_tables:
    if u['table_count'] > u['file_count']:
        print(f"For Company {u['company']} there are more records in table {u['entity']} there are {u['table_count']} than in the files {u['file_count']}")
        test_passed = False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if test_passed:
    print("All Tests Passed!!!!!!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
