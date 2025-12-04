# Data-Warehouse
CCG's datawarehouse GIT repo, contains the following.

- DataExtraction
    - BC2ADLS configurations
    - Historical Data extraction
        - Instructions on how to achieve it
        - Python scripts to manipulate data into BC object types where required
- Infrastructure folder 
    - Fabric 
        - Lakehouse configuration
        - Notebooks
        - Pipelines
        - Pyspark environments
        - Testing scripts
        - Utility scripts 
            - Historical field_mapping to BC data model
- Documentation
    - Excel spreadsheets to define keys/metadata for Bronze
- Workflowgeneration
    - workflow  - Generating the Bronze workflows from the Excel spreadsheets in the Documentation folder 
    - other - (bc_table_numbers)