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

#!/usr/bin/env python
# coding: utf-8

# ## field_mapping_export_notebook_with_priority
# 
# New notebook

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[1]:


#!/usr/bin/env python
# coding: utf-8

# ## Data field Mapping with Export
# 
# New notebook

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install fuzzywuzzy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[ ]:


from pyspark.sql.types import StructType, StructField, StringType
import re
import os
from pyspark.sql.functions import col
import pandas as pd
from fuzzywuzzy import process, fuzz
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[ ]:
priority = [
    "PaymentTerms",
    "Currency",
    "SalespersonPurchaser",
    "Location",
    "GLAccount",
    "GLEntry",
    "Customer",
    "CustLedgerEntry",
    "Vendor",
    "VendorLedgerEntry",
    "Item",
    "ItemLedgerEntry",
    "SalesInvoiceHeader",
    "SalesInvoiceLine",
    "PurchInvHeader",
    "PurchInvLine",
    "SalesHeader",
    "SalesLine",
    "PurchaseHeader",
    "PurchaseLine",
    "AccountingPeriod",
    "CustomerPostingGroup",
    "VendorPostingGroup",
    "InventoryPostingGroup",
    "UnitOfMeasure",
    "GenBusinessPostingGroup",
    "GenProductPostingGroup",
    "VATBusinessPostingGroup",
    "VATProductPostingGroup",
    "Employee",
    "FixedAsset",
    "FALedgerEntry",
    "FAPostingGroup",
    "FAPostingGroup",
    "FAClass",
    "FASubclass",
    "FALocation",
    "DepreciationBook",
    "FA DepreciationBook",
    "FARegister",
    "FAJournalLine",
    "FAReclassJournalLine",
    "FAPostingType",
    "Manufacturer",
    "ItemCategory",
    "ValueEntry",
    "WHTProductPostingGroup",
    "DIFOTSalesLine",
    "DIFOTShipmentLine",
    "RentalContract",
    "FundingBody",
    "ContractOrder",
    "Prescriber",
    "Brand",
    "Facility"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[ ]:
# CHANGE HERE - put in the source files you want to map
source_system = 'Xero'
source_other = "Files/history/xero/ansteys"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[ ]:
output_path = f"/tmp/{source_system}_to_BusinessCentral_Mapping.xlsx"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Don't change this
source_bc = "Files/deltas/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[ ]:


def drop_null_columns(df):
    null_cols = [c for c in df.columns if df.filter(col(c).isNotNull()).count() == 0]

    # Drop those columns
    return df.drop(*null_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[ ]:


def check_for_csvs(folder_path):
    if mssparkutils.fs.exists(folder_path):
        files = mssparkutils.fs.ls(folder_path)

        # Filter only CSV files
        csv_files = [f.path for f in files if f.name.lower().endswith(".csv")]

        if csv_files:
            # Read just the first CSV file
            first_csv = csv_files[0]
            return spark.read.option("header", True).csv(first_csv)

    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def order_dataframes(dataframes, priority):
    """
    Orders a list of (name, dataframe) tuples according to the priority list.
    Items not in the priority list are placed at the end in their original order.
    """
    # Map priority names to their index
    priority_index = {name: i for i, name in enumerate(priority)}

    # Sort key: items in priority get their index, others get a large number to go last
    def sort_key(item):
        name, _ = item
        return priority_index.get(name, len(priority) + 1)

    # Sort is stable, so unmatched items preserve their relative order
    return sorted(dataframes, key=sort_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[ ]:
manual_mappings = {
    "id": "no",
    "town/city": "city"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = StructType([
    StructField(f"{source_system}_Field", StringType(), True),
    StructField("BC_Field", StringType(), True),
    StructField("Primary Key", StringType(), True),  # or IntegerType() if pk is integer
    StructField("Foreign Key", StringType(), True),  # or IntegerType() if fk is integer
    StructField("Notes", StringType(), True)  # placeholder column
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dataframes = []

for folder in mssparkutils.fs.ls(source_bc):
    sheet_name = folder.name.split("-")[0].lower()
    print(f"Processing sheet: {sheet_name}")

    bc_folder_path = f"{source_bc}/{folder.name}/"
    other_folder_path = f"{source_other}/{sheet_name}"

    # Load BC CSV
    bc_df = check_for_csvs(bc_folder_path)
    if bc_df is None:
        print(f"No BC CSV found for {sheet_name}, creating empty DataFrame")
        bc_df = spark.createDataFrame([], StructType([]))
    else:
        bc_df = drop_null_columns(bc_df)

    # Load other CSV
    other_df = check_for_csvs(other_folder_path)
    if other_df is None:
        print(f"No other CSVs found for {sheet_name}, creating empty DataFrame")
        other_df = spark.createDataFrame([], StructType([]))
    else:
        other_df = drop_null_columns(other_df)

    # Clean column names
    cols1 = [re.sub(r'-\d+', '', c).replace(' ', '').lower() for c in other_df.columns]
    cols2 = [re.sub(r'-\d+', '', c).replace("$", '').lower() for c in bc_df.columns]

    # Fuzzy match columns
    matched = []
    only_in_1 = []
    only_in_2 = []

    for c1 in cols1:
        # Check manual mapping first
        if c1 in manual_mappings and manual_mappings[c1] in cols2:
            matched.append((c1, manual_mappings[c1]))
            continue

        # Safe fuzzy match
        result = process.extractOne(c1, cols2, scorer=fuzz.token_sort_ratio)
        if result is None:
            only_in_1.append((c1, None))
            continue

        best_match, score = result
        if score >= 80:
            matched.append((c1, best_match))
        else:
            only_in_1.append((c1, None))

    # Columns in bc_df not matched
    matched_cols2 = [b for _, b in matched if b is not None]
    for c2 in cols2:
        if any(c2 == manual_mappings.get(k) for k in manual_mappings.keys()):
            continue
        if c2 not in matched_cols2:
            only_in_2.append((None, c2))

    # Combine all matches
    data = matched + only_in_1 + only_in_2

    # Apply PK/FK logic
    final_data = []
    for src, bc in data:
        pk, fk = "False", "False"
        if bc:
            bc_clean = bc.lower()
            # Primary key: exact match on entryno/no/code
            if bc_clean in ["entryno", "no", "code"]:
                pk = "True"
            # Foreign key: ends with no/code but not primary key
            elif bc_clean.endswith("no") or bc_clean.endswith("code"):
                fk = "True"
        # Data Type left as None for now
        final_data.append((src, bc, pk, fk, None))

    # Create the DataFrame (even if empty) and convert to pandas
    df = spark.createDataFrame(final_data, schema).toPandas()
    print(f"Appending sheet '{sheet_name}' with {len(df)} rows")
    dataframes.append((sheet_name, df))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(len(dataframes))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(priority)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[ ]:
ordered_dataframes = order_dataframes(dataframes, priority)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[ ]:


with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    for sheet_name, df in ordered_dataframes:
        try:
            # Convert mapping to DataFrame
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"Finished processiong {sheet_name}")
        except Exception as e:
            print(f"❌ Error processing {sheet_name}: {e}")

lakehouse_path = f"Files/mapping/{source_system}_BusinessCentral_Mapping.xlsx"
mssparkutils.fs.cp(f"file:{output_path}", lakehouse_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
