# Mapping Tables to be in the datawarehouse
In each environment and for each company we have excel tab with the tables that are being used and whether they should be in the Datawarehouse.

# How to find out if a particular table is being used.
https://businesscentral.dynamics.com/<tenantId>/<environment>?page=8700

Export this Excel and filter it on the tables that have data.

# What is BC2ADLS?
It is an extraction tool to retrieve data from Business Central into a central location, this being Fabric for this project.

Details on the tool can be found here.

https://github.com/Bertverbeek4PS/bc2adls/blob/main/README.md

# How do I access this BC2ADLS extension?
https://businesscentral.dynamics.com/<tenantId>/<environment>?page=82560

# What are the XMLs for?
These are the data export configurations of the BC objects for each company.

# How do I export data from BC using this tool
* Navigate to the extension
* Configure the Fabric Environment 
    - Storage Type - Microsoft Fabric
    - Tenant ID -  will be the same as Business Central
    - Microsoft Fabric
      - Given the url - https://app.fabric.microsoft.com/groups/<workspaceGUID>/lakehouses/<lakehouseGUID>
        - Workspace - GUID 
        - Lakehouse - GUID 
    - App registration
        - This is a service principal that needs to be registered by CCG.
        - Both of these should be stored in KeyVault
            - Client ID (ApplicationID)
            - Client Secret
    - Other settings
        - Delete Table - Yes
        - Add delivered DateTime - Yes
        - Emit Telemetry - Yes
        - Skip row versioning - Yes
        - Export Company Database Tables - <company>

* Configure and execute the data extract
    - Import the xml file
    - Schema Export
    - Start Export

# What if there is a new company created and I want to import data.
Use the template xml file and import it in the BC2ADLS extension, make your changes and the export it again and save in this project.

# How do we configure each company export
This can be achieved in multiple ways.

* Forked version of BC2ADLS
* JobQueues
* PowerAutomate

This is still TBC.