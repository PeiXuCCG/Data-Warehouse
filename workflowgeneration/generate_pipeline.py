import json
from copy import deepcopy
import generate_activities 
import sys


TEMPLATE = {
    "properties": {
        "activities": [
            {
                "type": "ForEach",
                "typeProperties": {
                    "isSequential": True,
                    "items": {
                        "value": "@pipeline().parameters.job_configuration",
                        "type": "Expression"
                    },
                    "activities": []
                },
                "name": "job_name",
                "dependsOn": []
            }
        ],
        "parameters": {
            "target_schema": {
                "type": "string",
                "defaultValue": ""
            },
            "target_db": {
                "type": "string",
                "defaultValue": ""
            },
            "job_configuration": {
                "type": "array",
                "defaultValue": []
            },
            "dry_run": {
                "type": "bool",
                "defaultValue": "false"
            },
            "wait_seconds": {
                "type": "int",
                "defaultValue": 5
            },
            "skip_activities": {
                "type": "array",
                "defaultValue": []
            }
        }
    }
}


def build_pipeline_json(job_config, job_name, source_lakehouse, source_schema, target_lakehouse, target_schema, table_prefix, notebook_id):
    """
    Build updated ADF pipeline JSON from template.
    job_config: list (job configuration array)
    job_name: string
    target: string
    target_schema: string
    """

    pipeline_json = deepcopy(TEMPLATE)

    # Update job name
    pipeline_json["properties"]["activities"][0]["name"] = job_name

    # Set target values
    pipeline_json["properties"]["parameters"]["target_schema"]["defaultValue"] = target_lakehouse
    pipeline_json["properties"]["parameters"]["target_db"]["defaultValue"] = target_schema

    pipeline_json["properties"]["parameters"]["wait_seconds"]["defaultValue"] = 5
    
    # Set job configuration parameter
    pipeline_json["properties"]["parameters"]["job_configuration"]["defaultValue"] = job_config

    generated = generate_activities.generate(path_to_excel, job_config, source_lakehouse,source_schema, target_lakehouse, target_schema, table_prefix, notebook_id)

    pipeline_json["properties"]["activities"][0]["typeProperties"]["activities"] = generated


    return pipeline_json


if __name__ == "__main__":
    # Example usage:
    
    bc_job_name = "Load_Data"
    bc_notebook_id =  "11363096-0db8-9724-4979-fb4b00909b73" 
    bc_table_prefix = "bc" 
    bc_job_configuration = [
        "BC"
    ]

    historical_job_name = "Load_Historical_Data"
    historical_notebook_id = "91dad485-d600-8560-4fea-29a406f901ff"
    historical_table_prefix = "historical" 
    historical_job_configuration = [
          "Xero",
          "Myob",
          "Netsuite",
          "Hirepos",
          "Lightspeed",
          "Windward",
          "Natsoft",
          "Ostendo"
    ]

    source_lakehouse = "lh_bronze"
    source_schema = "raw"
    target_lakehouse = "lh_bronze"
    target_schema = "bronze"
    

    path_to_excel = "../documentation/historical_bronze_workflow_mapping.xlsx"
    output_path = "pipeline.json"
      
    workflow_type = sys.argv[1]

    if workflow_type == "bc":
        job_configuration = bc_job_configuration
        job_name = bc_job_name
        notebook_id = bc_notebook_id
        table_prefix = bc_table_prefix
    elif workflow_type == "historical":
        job_configuration = historical_job_configuration
        job_name = historical_job_name
        notebook_id = historical_notebook_id
        table_prefix = historical_table_prefix
    else:
        raise Exception("Unknown workflow generation")

    output = build_pipeline_json(bc_job_configuration, job_name, source_lakehouse, source_schema, target_lakehouse, target_schema, table_prefix, notebook_id)

    # write to disk
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"JSON written to {output_path}")
