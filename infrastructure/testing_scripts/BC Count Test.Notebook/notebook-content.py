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

# COMPARE THE ROW COUNTS ON THE BC OBJECTS WITH WHAT IS ON PAGE 8700 in BC

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.functions import col
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

company = [
  ("Retail Country Care Group","bc"),
  ("K Care Holdings Pty Ltd","bc"),
  ("CCG Contracts","bc"),
  ("Ind. Healthcare Solutions","bc"),
  ("Management Country Care Group","bc"),
  ("QLD Rehab Equipment","bc"),
  ("CCGA Holdings","bc"),
  ("CCGA Bidco Pty Ltd", "bc")
  # ("Healthsaver", "MYOB"),
  # ("Chair Doctor", "MYOB"),
  # ("Ergo", "MYOB"),
  # ("FisherLane Mobility", ["MYOB", "ITV"]),
  # ("Vital_Liviing", "Netsuite"),
  # ("Ansteys",["Xero", "Lightspeed"]),
  # ("Homecare Equipment", ["Xero", "HirePOS"]),
  # ("Lakeside Mobility", ["Xero", "HirePOS"]),
  # ("Open Mobility",["Xero", "Lightspeed"]),
  # ("Uccello Design UK","Xero"),
  # ("Uccello Marketing AU","Xero"),
  # ("Uccello Marketing EU","Xero"),
  # ("WILLAID",["MYOB", "Ostendo"]),
  # ("MCLEANS","NATSOFT"),
  # ("EDEN", "Netsuite"),
  # ("MERITS","Netsuite"),
  # ("TCCG & HomeModifications", "Windward")
]

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

# CELL ********************

entities = ["customer", 
            "item",
            "salesinvoiceheader", 
            "salesinvoiceline",
            "purchinvheader",
            "purchinvline",
            "glentry",
            "glaccount",
            "custledgerentry",
            "vendorledgerentry",
            "itemledgerentry",
            "vendor",
            "valueentry",
            # These are BC objects
            "location",
            "paymentterms",
            "currency",
            "salespersonpurchaser",
            "customerpostinggroup",
            "vendorpostinggroup",
            "inventorypostinggroup",
            "genbusinesspostinggroup",
            "genproductpostinggroup",
            "vatbusinesspostinggroup",
            "vatproductpostinggroup",
            "employee",
            "fixedasset",
            "faledgerentry",
            "fapostinggroup",
            "faclass",
            "fasubclass",
            "falocation",
            "depreciationbook",
            "fadepreciationbook",
            "faregister",
            "fajournalline",
            "fareclassjournalline",
            "fapostingtype",
            "manufacturer",
            "itemcategory",
            "whtproductpostinggroup",
            "difotsalesline",
            "difotshipmentline",
            "rentalcontract",
            "fundingbody",
            "contractorder"
        ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = []

for name, system in company:   # your original "for name, system in company:"
    print(f"------------ Starting review of {name} ------------------")

    if system == "bc":
        for e in entities:
            table = f"raw.{system}_{e}"

            if spark.catalog.tableExists(table):
                df = spark.sql(f"""
                    SELECT COUNT(DISTINCT *) as cnt
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

display(pivot_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
