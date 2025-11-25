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
source_system = 'myob'
source_other = "Files/history/myob/healthsaver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# In[ ]:
output_path = f"/tmp/{source_system}_BusinessCentral_Mapping.xlsx"

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

# In[ ]:
def order_dataframes(dataframes, priority, threshold=60):
    # Copy the list so we don’t mutate the original
    remaining = dataframes.copy()
    ordered = []

    for p in priority:
        # Fuzzy match best sheet_name in remaining
        best_match, score = process.extractOne(p, [name for name, _ in remaining])
        if score >= threshold:
            # Pull that one out and append to ordered
            match = next(item for item in remaining if item[0] == best_match)
            ordered.append(match)
            remaining.remove(match)

    # Add anything left that didn’t match
    ordered.extend(remaining)

    return ordered

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

# In[ ]:
dataframes = []

# Define schema
schema = StructType([
    StructField(f"{source_system} Field", StringType(), True),
    StructField("Business Central Field", StringType(), True),
    StructField("Primary Key", StringType(), True),
    StructField("Foreign Key", StringType(), True),
    StructField("Data Type", StringType(), True)
])

for folder in mssparkutils.fs.ls(source_bc):
    sheet_name = folder.name.split("-")[0].lower()
    print(sheet_name)
    other_folder_path = f"{source_other}/{sheet_name}"
    bc_folder_path = f"{source_bc}/{folder.name}"
    
    bc_df = check_for_csvs(bc_folder_path)
    if bc_df is None:
        continue
    else:    
        bc_df = drop_null_columns(bc_df)
        other_df = check_for_csvs(other_folder_path)
        if other_df is None:
            other_df = spark.createDataFrame([], StructType([]))
    
    # Clean column names
    cols1 = [re.sub(r'-\d+', '', c).replace(' ', '').lower() for c in other_df.columns]
    cols2 = [re.sub(r'-\d+', '', c).replace("$", '').lower() for c in bc_df.columns]
    
    matched = []
    only_in_1 = []
    only_in_2 = []

    # Fuzzy match each col in cols1 to best match in cols2
    for c1 in cols1:
        # Check if this column has a manual mapping
        if c1 in manual_mappings and manual_mappings[c1] in cols2:
            matched.append((c1, manual_mappings[c1]))
            continue
        
        best_match, score = process.extractOne(c1, cols2, scorer=fuzz.token_sort_ratio)
        if score >= 80:  # threshold, can adjust
            matched.append((c1, best_match))
        else:
            only_in_1.append((c1, None))

    # Find cols2 that did not match anything
    matched_cols2 = [b for _, b in matched]
    for c2 in cols2:
        # Skip if it's already manually mapped
        if any(c2 == manual_mappings.get(k) for k in manual_mappings.keys()):
            continue
        if c2 not in matched_cols2:
            only_in_2.append((None, c2))

    # Combine results
    data = matched + only_in_1 + only_in_2


    # Apply PK/FK logic
    final_data = []
    for src, bc in data:
        pk = "False"
        fk = "False"

        if bc is not None:
            bc_clean = bc.lower()

            # Primary key: exact match entryno or no
            if bc_clean in ["entryno", "no", "code"]:
                pk = "True"

            # Foreign key: ends with "no" but not primary key fields
            elif bc_clean.endswith("no") or bc_clean.endswith("code"):
                fk = "True"

        # Append row (Data Type left empty for now)
        final_data.append((src, bc, pk, fk, None))

    # Create the DataFrame
    df = spark.createDataFrame(final_data, schema).toPandas()
    dataframes.append((sheet_name, df))

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
