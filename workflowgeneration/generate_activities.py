import pandas as pd
import json


def clean_key_list(value: str) -> str:
    if not isinstance(value, str):
        return "[]"
    cleaned = [p.strip() for p in value.split(",") if p.strip()]
    return json.dumps(cleaned)


def build_busines_keys(value: str) -> str:
    if not isinstance(value, str):
        return "[]"
    cleaned = [p.strip() for p in value.split(",") if p.strip()]
    return json.dumps(cleaned)


def build_source_fk(value: str) -> str:
    if not isinstance(value, str):
        return "[]"
    parts = [p.strip() for p in value.split(",") if p.strip()]
    fk_list = [{p: [p, "originating_company", "source_system"]} for p in parts]
    return json.dumps(fk_list, separators=(",", ":"))


# -----------------------------------------------------------------------
# FULL GENERATOR SCRIPT WITH WAIT ACTIVITY AS A PIPELINE PARAMETER
# -----------------------------------------------------------------------
def generate(df,
             source_system,
             source_lakehouse,
             source_schema,
             target_lakehouse,
             target_schema,
             table_prefix,
             notebook_id):



    output = []
    previous_step = None      # Tracks notebook OR wait activity
    last_index = df.index[-1]

    for idx, row in df.iterrows():

        entity = row["Entity"]

        if "BC" not in source_system:
            source_key = row["SchemaBridge_Source_Key"]

        source_primary_keys = clean_key_list(row["Bronze_Source_Keys"])
        dedup = clean_key_list(row["Bronze_Deduplication_Keys"])
        source_fk = build_source_fk(row["Bronze_Source_Foreign_Keys"])
        business_keys = build_busines_keys(row["Bronze_Partition_Keys"])



        # ==================================================================
        # NOTEBOOK ACTIVITY
        # ==================================================================
        notebook_name = f"{entity}_Activity"

        notebook_depends = []
        if previous_step:
            notebook_depends = [{
                "activity": previous_step,
                "dependencyConditions": ["Completed"]
            }]

        notebook_activity = {
            "type": "TridentNotebook",
            "typeProperties": {
                "notebookId": notebook_id,
                "workspaceId": "00000000-0000-0000-0000-000000000000",
                "parameters": {
                    "source_system": {
                        "value": {"value": "@item()", "type": "Expression"},
                        "type": "string"
                    },
                    "source_lakehouse": {"value": source_lakehouse, "type": "string"},
                    "source_schema": {"value": source_schema, "type": "string"},
                    "source_table": {
                        "value": {
                            "value": f"@concat(item(),'_{entity.lower()}')",
                            "type": "Expression"
                        },
                        "type": "string"
                    },
                    "target_lakehouse": {"value": target_lakehouse, "type": "string"},
                    "target_schema": {"value": target_schema, "type": "string"},
                    "target_table": {
                        "value": f"{table_prefix}_{entity.lower()}",
                        "type": "string"
                    },
                    "source_primary_keys": {"value": source_primary_keys, "type": "string"},
                    "source_foreign_keys": {"value": source_fk, "type": "string"},
                    "deduplicate_fields": {"value": dedup, "type": "string"},
                    "business_keys": {"value": business_keys, "type": "string"},
                    "skip_activities": { 
                        "value": {
                            "value": "@string(pipeline().parameters.skip_activities)",
                            "type": "Expression"
                        },
                        "type": "string"
                    },
                    "activity": {"value": f"{entity}", "type": "string"},
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
            "name": notebook_name,
            "dependsOn": notebook_depends
        }

        if "BC" not in source_system:
            notebook_activity["typeProperties"]["parameters"]["source_key"] = {
                "value": source_key,
                "type": "string"
            }

        output.append(notebook_activity)

        # ==================================================================
        # WAIT ACTIVITY (USING PIPELINE PARAMETER)
        # ==================================================================
        if idx != last_index:

            wait_name = f"Wait_For_Spark_To_Stop_{entity}"

            wait_activity = {
                "type": "Wait",
                "typeProperties": {
                    "waitTimeInSeconds": {
                        "value": "@pipeline().parameters.wait_seconds",
                        "type": "Expression"
                    }
                },
                "name": wait_name,
                "dependsOn": [
                    {
                        "activity": notebook_name,
                        "dependencyConditions": ["Completed"]
                    }
                ]
            }

            output.append(wait_activity)
            previous_step = wait_name  # only advance previous_step if wait exists

        else:
            # Last item → next activity depends on notebook, not a wait
            previous_step = notebook_name

    return output
