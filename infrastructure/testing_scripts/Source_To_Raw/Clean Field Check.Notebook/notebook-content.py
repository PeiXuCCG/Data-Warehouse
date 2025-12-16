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

# CHANGE THIS FOR YOUR TEST
source_system = "Xero"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema_name = "raw"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns = []


rules_df = spark.sql(f"select * from dbo.cleaning_rules_set where rule_set = '{source_system}'")

for row in rules_df.collect():
    columns.append({row["source_columns"]: row["target_column"]})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = []

tables_df = spark.sql(f"SHOW TABLES IN lh_bronze.{schema_name}")

tables = tables_df.collect()

for t in tables:
    table_name = t.tableName
    full_name = f"lh_bronze.{schema_name}.{table_name}"

    if source_system.lower() in table_name:
        print(table_name)
        df = spark.sql(f"SELECT * FROM {full_name}")

        for c in columns:
            key, value = list(c.items())[0]

            # Skip if source column (key) does NOT exist
            if key not in df.columns:
                continue

            # Source column exists → now check target column
            exists = value in df.columns

            # Append one result per mapping
            results.append({
                "table": full_name,
                "source_column": key,
                "target_column": value,
                "exists": exists
            })


          

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

paased = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for r in results:
    if not r["exists"]:
        passed = False
        print(f"{r['table']} has rule for {r['source_column']}, missing {r['target_column']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if passed:
    print("All tests passed!!!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
