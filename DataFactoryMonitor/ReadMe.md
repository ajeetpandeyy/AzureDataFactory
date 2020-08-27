# Data Factory Auto Refresh Monitoring

This web app demonstrates a way of querying the Data Factory REST API using the [Python SDK](https://docs.microsoft.com/en-us/python/api/azure-mgmt-datafactory/azure.mgmt.datafactory?view=azure-python).

## To get Started

Create a .env file in the parent directory holding the app folder.  The .env should contain the following environment variables.

    WEBSITES_CONTAINER_START_TIME_LIMIT=500
    WEBSITES_PORT=5000
    TENANT_ID=<YOUR TENANT ID>
    SUBSCRIPTION_ID=<YOUR SUBSCRIPTION ID>
    CLIENT_ID=<YOUR SERVICE PRINCIPAL ID>
    CLIENT_KEY=<YOUR SERVICE PRINCIPAL KEY>
    RESOURCE_GROUP=<RESOURCE GROUP NAME HOLDING THE DATA FACTORY>
    DATA_FACTORY_NAME=<NAME OF DATA FACTORY TO QUERY>
    TIME_ZONE=US/Central # Get available time zones by following https://stackoverflow.com/a/13867319/950280

To generate the `CLIENT_ID` and `CLIENT_KEY`, follow the instructions on creating a [Service Principal](https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal) and assign that Service Principal Contributor status to the Data Factory you plan on querying.


## About
There are three main pages:
1. `/index` to display the pipelines in the past 7 days that are the latest run (i.e. only includes the most recent re-run) and have failed or succeeded.
1. `/all` to display all pipelines regardless of re-run history.
1. `/failed` to display only the latest run of failed pipelines.

Then are four main api calls:
1. `/api/pipelines/<start>/sparse` which queries just the high level pipelineRun information and not the activities underneath it (for faster querying).  Latest Pipeline Run only.
1. `/api/pipelines/<start>/full` which queries the high level pipelineRun information and includes the activities underneath it (for more detailed results).  Not used in the app but defined for other applications that you may develop.  Latest Pipeline Run only.
1. `/api/pipelines/<start>/full/all` Same as its `full` predecessor but includes all pipeline runs and not just the latest run.
1. `/api/pipeline/<run_id>/` queries a single pipeline just based on the run id guid.  This is used by the `logic.js` to add activity detail in the web app.

