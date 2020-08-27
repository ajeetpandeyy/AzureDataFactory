import os

import pytz
from datetime import datetime

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *

# Loading the .env file
from os.path import join, dirname
from dotenv import load_dotenv

top_level_dir, _ = os.path.split(dirname(__file__))
dotenv_path = join(top_level_dir, '.env')
load_dotenv(dotenv_path)

_TENANT_ID = os.getenv("TENANT_ID")
_SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID")
_CLIENT_ID = os.environ.get("CLIENT_ID")
_CLIENT_KEY = os.environ.get("CLIENT_KEY")
_RESOURCE_GROUP = os.environ.get("RESOURCE_GROUP")
_DATA_FACTORY_NAME = os.environ.get("DATA_FACTORY_NAME")
_TIME_ZONE = os.environ.get("TIME_ZONE")

ISO8601_FORMAT = "%Y%m%dT%H:%M:%S"

credentials = ServicePrincipalCredentials(
    client_id=_CLIENT_ID, 
    secret=_CLIENT_KEY, 
    tenant=_TENANT_ID
)

adf_client = DataFactoryManagementClient(credentials, _SUBSCRIPTION_ID)


def query_activities(run_id, runfilterparam):
    activities = adf_client.activity_runs.query_by_pipeline_run(
        _RESOURCE_GROUP, 
        _DATA_FACTORY_NAME,
        run_id=run_id,
        filter_parameters=runfilterparam).value

    pipe_activities = []
    for activity in activities:
        output_activity = {
            "activity_name":activity.activity_name,
            "status": activity.status,
            "input":activity.input,
            "output":activity.output,
            "error":activity.error
        }
        if activity.activity_type == "DatabricksNotebook":
            output_activity["databricks_url"] =  activity.output.get("runPageUrl",None)
        pipe_activities.append(output_activity)
    
    return pipe_activities

def extract_standard_pipeline_data(pipelineRun):
    return {
            "run_id":pipelineRun.run_id,
            "pipeline_name":pipelineRun.pipeline_name,
            "status":pipelineRun.status,
            "message":pipelineRun.message,
            "annotations": pipelineRun.additional_properties["annotations"],
            "parameters":pipelineRun.parameters,
            "run_start":pipelineRun.run_start,
            "run_end":pipelineRun.run_end,
            "adf_url": ''.join([
                "https://adf.azure.com/monitoring/pipelineruns/",
                pipelineRun.run_id, 
                "?factory=%2Fsubscriptions%2F",
                _SUBSCRIPTION_ID, 
                "%2FresourceGroups%2F", _RESOURCE_GROUP, 
                "%2Fproviders%2FMicrosoft.DataFactory%2Ffactories%2F",
                _DATA_FACTORY_NAME]
            ),
            "activities":[]
        }

def query_single_pipe(run_id):
    pipe = adf_client.pipeline_runs.get(
        _RESOURCE_GROUP, 
        _DATA_FACTORY_NAME, 
        run_id
    )
    rpf = RunFilterParameters(
        last_updated_after=pipe.run_start,
        last_updated_before=pipe.run_end
    )

    output_pipe = extract_standard_pipeline_data(pipe)
    
    pipe_activities = query_activities(pipe.run_id, rpf)
    
    output_pipe["activities"] = pipe_activities

    return output_pipe


def query_all_pipes(start, end = datetime.now(), include_activities=False, latest_only=True):
    time_zone = pytz.timezone(_TIME_ZONE)

    start = time_zone.localize(datetime.strptime(start, ISO8601_FORMAT))
    end = time_zone.localize(end)

    rpf = RunFilterParameters(
        last_updated_after=start,
        last_updated_before=end,
        order_by=[RunQueryOrderBy(order_by="RunStart", order="DESC")]
    )  
    results = adf_client.pipeline_runs.query_by_factory(
        _RESOURCE_GROUP, 
        _DATA_FACTORY_NAME, 
        filter_parameters=rpf
    )

    output = []
    for pipe in results.value:
        if latest_only and not pipe.additional_properties.get("isLatest"):
            continue
        
        output_pipe = extract_standard_pipeline_data(pipe)
        if include_activities:
            pipe_activities = query_activities(pipe.run_id, rpf)
            output_pipe["activities"] = pipe_activities
        elif "activities" in output_pipe:
            _ = output_pipe.pop("activities")
            
        output.append(output_pipe)
    
    return output