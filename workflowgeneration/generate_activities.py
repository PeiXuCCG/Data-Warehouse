import pandas as pd
import json


def clean_key_list(value: str) -> str:
    if not isinstance(value, str):
        return "[]"

    # split by comma
    parts = value.split(",")

    # strip whitespace and remove blanks
    cleaned = [p.strip() for p in parts if p.strip()]

    # return JSON-style array string
    return json.dumps(cleaned)


def build_busines_keys(value: str) -> str:
    if not isinstance(value, str):
        return "[]"
    
    parts = value.split(",")

    # strip whitespace and remove blanks
    cleaned = [p.strip() for p in parts if p.strip()]

    # return JSON-style array string
    return json.dumps(cleaned)


def build_source_fk(value: str) -> str:
    if not isinstance(value, str):
        return "[]"

    # split on commas and clean whitespace
    parts = [p.strip() for p in value.split(",") if p.strip()]

    # turn each value into a dict:
    # { "<key>": ["<key>", "company", "source_system"] }
    fk_list = [
        { p: [p, "company", "source_system"] }
        for p in parts
    ]

    # return JSON string
    return json.dumps(fk_list, separators=(",", ":"))

# --- Load Excel ---
def generate(path_to_file,  source_system, source_lakehouse, source_schema, target_lakehouse, target_schema):

    df = pd.read_excel(path_to_file)  
    # Expected columns:
    # entity, source_key, source_primary_keys, deduplication_key, source_foreign_keys, business_key

    output = []

    previous_activity_name = None 

    for _, row in df.iterrows():

        entity = row["Entity"]
        source_key = row["SchemaBridge_Source_Key"]
        source_primary_keys = clean_key_list(row["Bronze_Source_Keys"])        # keep EXACT formatting
        dedup = clean_key_list(row["Bronze_Deduplication_Keys"])
        source_fk = build_source_fk(row["Bronze_Source_Foreign_Keys"])                  # keep EXACT formatting
        business_keys = build_busines_keys(row["Bronze_Partition_Keys"])                  # keep EXACT formatting

        activity_name = f"{entity}_Activity"

        # Build dependency
        if previous_activity_name is None:
            depends = []
        else:
            depends = [
                {
                    "activity": previous_activity_name,
                    "dependencyConditions": ["Succeeded"]
                }
            ]

        record = {
            "type": "TridentNotebook",
            "typeProperties": {
                "notebookId": "91dad485-d600-8560-4fea-29a406f901ff",
                "workspaceId": "00000000-0000-0000-0000-000000000000",
                "parameters": {
                    "source_system": {
                        "value": {
                            "value": "@item()",
                            "type": "Expression"
                        },
                        "type": "string"
                    },
                    "source_lakehouse": {
                        "value": f"{source_lakehouse}",
                        "type": "string"
                    },
                    "source_schema": {
                        "value": f"{source_schema}",
                        "type": "string"
                    },
                    "source_table": {
                        "value": {
                            "value": f"@concat(item(),'_{entity.lower()}')",
                            "type": "Expression"
                        },
                        "type": "string"
                    },
                    "target_lakehouse": {
                        "value": f"{target_lakehouse}",
                        "type": "string"
                    },
                    "target_schema": {
                        "value": f"{target_schema}",
                        "type": "string"
                    },
                    "target_table": {
                        "value": f"historical_{entity.lower()}",
                        "type": "string"
                    },
                    "source_key": {
                        "value": source_key,
                        "type": "string"
                    },
                    "source_primary_keys": {
                        "value": source_primary_keys,
                        "type": "string"
                    },
                    "source_foreign_keys": {
                        "value": source_fk,
                        "type": "string"
                    },
                    "deduplicate_keys": {
                        "value": dedup,
                        "type": "string"
                    },
                    "business_keys": {
                        "value": business_keys,
                        "type": "string"
                    },
                    "dry_run": {
                       "value": {
                            "value": "@pipeline().parameters.dry_run",
                            "type": "Expression"
                        },
                        "type": "bool"
                    }
                }
            },
            "policy": {
                "timeout": "0.12:00:00",
                "retry": 0,
                "retryIntervalInSeconds": 30,
                "secureInput": False,
                "secureOutput": False
            },
            "name": activity_name,
            "dependsOn": depends
        }

        output.append(record)

        # update previous activity pointer
        previous_activity_name = activity_name


    return output
