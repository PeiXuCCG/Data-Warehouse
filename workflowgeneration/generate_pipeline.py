import json
from copy import deepcopy
import generate_activities 


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
            }
        }
    }
}


def build_pipeline_json(job_config, job_name, target, target_schema):
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
    pipeline_json["properties"]["parameters"]["target_schema"]["defaultValue"] = target
    pipeline_json["properties"]["parameters"]["target_db"]["defaultValue"] = target_schema

    # Set job configuration parameter
    pipeline_json["properties"]["parameters"]["job_configuration"]["defaultValue"] = job_config

    generated = generate_activities.generate(path_to_excel)

    pipeline_json["properties"]["activities"][0]["typeProperties"]["activities"] = generated


    return pipeline_json


if __name__ == "__main__":
    # Example usage:
    job_configuration = [
          "Xero",
          "Myob",
          "Netsuite",
          "Hirepos",
          "Lightspeed",
          "Windward",
          "Natsoft",
          "Ostendo"
    ]
    job_name = "Load_Historical_Data"
    target = "lh_bronze"
    target_schema = "bronze"
    path_to_excel = "../documentation/historical_bronze_workflow_mapping.xlsx"
    output_path = "pipeline.json"

    output = build_pipeline_json(job_configuration, job_name, target, target_schema)

    # write to disk
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"JSON written to {output_path}")
