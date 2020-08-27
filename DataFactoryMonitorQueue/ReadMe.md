# ADF Monitoring with Your Own Queue

## local.settings.json

    {
    "IsEncrypted": false,
    "Values": {
        "AzureWebJobsStorage": "UseDevelopmentStorage=true",
        "FUNCTIONS_WORKER_RUNTIME": "python",
        "TENANT_ID": "",
        "SUBSCRIPTION_ID": "",
        "CLIENT_ID": "",
        "CLIENT_KEY": "",
        "RESOURCE_GROUP": "",
        "DATA_FACTORY_NAME": "",
        "TIME_ZONE": "",
        "SQLDB_CONNECTION": ""
        }
    }

* TIME_ZONE should be a valid `pytz` time zone (see [here](https://stackoverflow.com/questions/13866926/is-there-a-list-of-pytz-timezones))
* SQLDB_CONNECTION should be an ODBC connection using SQL Authentication for `pyodbc`
* CLIENT_ID and CLIENT_KEY should be for a Service Principal with the ability to read and execute Data Factory Pipelines (see [here](https://docs.microsoft.com/en-us/azure/data-factory/concepts-roles-permissions)).