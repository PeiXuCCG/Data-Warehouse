import json
from copy import deepcopy
import generate_activities 
import sys
import pandas as pd
import numpy as np


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


def build_pipeline_jsons(output_path, job_config, job_name, source_lakehouse, source_schema, target_lakehouse, target_schema, table_prefix, notebook_id):
    """
    Build updated ADF pipeline JSON from template.
    job_config: list (job configuration array)
    job_name: string
    target: string
    target_schema: string
    """

    df = pd.read_excel(path_to_excel)

    count = 0
    chunk_size = 60
    chunks = [
        df[i:i + chunk_size]
        for i in range(0, len(df), chunk_size)
    ]

    for chunk in chunks:
        count = count + 1

        pipeline_json = deepcopy(TEMPLATE)

        # Update job name
        pipeline_json["properties"]["activities"][0]["name"] = job_name

        # Set target values
        pipeline_json["properties"]["parameters"]["target_schema"]["defaultValue"] = target_lakehouse
        pipeline_json["properties"]["parameters"]["target_db"]["defaultValue"] = target_schema

        pipeline_json["properties"]["parameters"]["wait_seconds"]["defaultValue"] = 5
        
        # Set job configuration parameter
        pipeline_json["properties"]["parameters"]["job_configuration"]["defaultValue"] = job_config

        generated = generate_activities.generate(chunk, job_config, source_lakehouse,source_schema, target_lakehouse, target_schema, table_prefix, notebook_id)

        pipeline_json["properties"]["activities"][0]["typeProperties"]["activities"] = generated


        # write to disk
        with open(f"{output_path}_{count}.json", "w", encoding="utf-8") as f:
            json.dump(pipeline_json, f, ensure_ascii=False, indent=2)

        print(f"JSON written to {output_path}")


if __name__ == "__main__":
    # Example usage:
    workflow_type = sys.argv[1]

    bc_job_name = "Load_Data"
    bc_notebook_id =  "11363096-0db8-9724-4979-fb4b00909b73" 
    bc_table_prefix = "bc" 
    bc_job_configuration = [
        "BC"
    ]
    bc_path_to_excel = "../documentation/workflow/bc_bronze_workflow_mapping.xlsx"

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
    historical_path_to_excel = "../documentation/workflow/historical_bronze_workflow_mapping.xlsx"

    contracts_job_name = "Contracts_Load_Data"
    contracts_job_configuration = ["Contracts"]
    contracts_notebook_id =  "d3c2a0ef-cc3b-862b-4552-5ea28821bd0c" 
    contracts_table_prefix = "contracts" 
    contracts_job_configuration = [
        "Contracts"
    ]
    contracts_path_to_excel = "../documentation/workflow/contracts_bronze_workflow_mapping.xlsx"


    target_lakehouse = "lh_bronze"
    target_schema = "bronze"
    

    
    output_path = "pipeline-content"
      
   

    if workflow_type == "bc":
        job_configuration = bc_job_configuration
        job_name = bc_job_name
        notebook_id = bc_notebook_id
        table_prefix = bc_table_prefix
        path_to_excel = bc_path_to_excel
        source_lakehouse = "lh_bronze"
        source_schema = "raw"
    elif workflow_type == "historical":
        job_configuration = historical_job_configuration
        job_name = historical_job_name
        notebook_id = historical_notebook_id
        table_prefix = historical_table_prefix
        path_to_excel = historical_path_to_excel
        source_lakehouse = "lh_bronze"
        source_schema = "raw"
    elif workflow_type == "contracts":
        job_configuration = contracts_job_configuration
        job_name = contracts_job_name
        notebook_id = contracts_notebook_id
        table_prefix = contracts_table_prefix
        path_to_excel = contracts_path_to_excel
        source_lakehouse = "lh_bronze"
        source_schema = "raw"  
    else:
        raise Exception("Unknown workflow generation")

    build_pipeline_jsons(output_path, job_configuration, job_name, source_lakehouse, source_schema, target_lakehouse, target_schema, table_prefix, notebook_id)


