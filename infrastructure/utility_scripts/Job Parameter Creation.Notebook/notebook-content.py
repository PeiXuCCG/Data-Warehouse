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

from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
base_path = "Files/deltas/"
prefix = "hirepos"


entries = []

for f in mssparkutils.fs.ls(base_path):
    if f.isDir:  # only subfolders
        folder_name = f.name               # e.g. AccountingPeriod-50
        table_name = folder_name.split("-")[0].lower()   # accountingperiod
        target_table = f"{prefix}_{table_name}"
        source_path = f"{folder_name}"

        entries.append({
            "target_table": f"{target_table}",
            "source_path": f"{source_path}"
        })

print(entries)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lst = [
    "accounttype",
    "globaldimension1code",
    "globaldimension2code",
    "accountcategory",
    "sourcecurrencycode",
    "sourcecurrencyposting",
    "consoltranslationmethod",
    "genpostingtype",
    "genbuspostinggroup",
    "genprodpostinggroup",
    "taxareacode",
    "taxgroupcode",
    "vatbuspostinggroup",
    "vatprodpostinggroup",
    "defaulticpartnerglaccno",
    "defaultdeferraltemplatecode",
    "apiaccounttype",
    "whtbusinesspostinggroup"
    "whtproductpostinggroup"
]


result = "[\\n"
for item in lst:
    result += (
        "    {\\\"" + item + "\\\": [\\\"" + item + "\\\", \\\"source_system\\\", \\\"company\\\"]},\\n"
    )
result = result.rstrip(",\\n") + "\\n]"

print(result)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
