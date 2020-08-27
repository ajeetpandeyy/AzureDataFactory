import os

import pytz
from datetime import datetime

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *

# Loading the .env file
from os.path import join, dirname

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

class ADFManagement():
    def __init__(self):
        self.adf_client = DataFactoryManagementClient(credentials, _SUBSCRIPTION_ID)

    def extract_standard_activities(self, activity):
        """
        Pulls out the important information in a consistent way for
        either a activity run (in progress or completed) or a 
        activity (not executed, present version of activity in the pipeline).

        :param activity:
        :type 
            ~azure.mgmt.datafactory.models.Activity or 
            ~azure.mgmt.datafactory.models.ActivityRun:
        :rtype dict(str, str):
        """
        output_activity = {
                "activity_name":None,
                "linked_services":None,
                "status": None,
                "input":None,
                "output":None,
                "error":None
            }

        linked_service_reference = set()

        if activity.type == "SetVariable":
            # Does not contain any Linked Service references
            pass
        elif activity.type == "Copy":
            # Extract input and output's dataset's linked services
            for dataset in activity.inputs + activity.outputs:
                dataset_resource = self.adf_client.datasets.get(
                    resource_group_name = _RESOURCE_GROUP, 
                    factory_name = _DATA_FACTORY_NAME, 
                    dataset_name = dataset.reference_name)
                
                local_linked_service = dataset_resource.properties\
                        .linked_service_name.reference_name
                linked_service_reference.add(local_linked_service)

        elif activity.linked_service_name:
            linked_service_reference = [activity.linked_service_name.reference_name]
        
        output_activity.update({"linked_services":list(linked_service_reference)})
        
        try:
            output_activity.update({
                "activity_name":activity.activity_name,
                "status": activity.status,
                "input":activity.input,
                "output":activity.output,
                "error":activity.error
            })
            if activity.activity_type == "DatabricksNotebook" and isinstance(activity.output, dict):
                output_activity["databricks_url"] =  activity.output.get("runPageUrl",None)
        except:
            output_activity.update({
                "activity_name":activity.name
            })

        return output_activity

    
    def query_activities(self, run_id, runfilterparam):
        activities = self.adf_client.activity_runs.query_by_pipeline_run(
            _RESOURCE_GROUP, 
            _DATA_FACTORY_NAME,
            run_id=run_id,
            filter_parameters=runfilterparam).value

        pipe_activities = [extract_standard_activities(activity) for activity in activities]
        
        return pipe_activities

    @staticmethod
    def extract_standard_pipeline_data(pipelineRun):
        """
        Pulls out the important information in a consistent way for
        either a pipeline run (in progress or completed) or a 
        pipeline resource (not executed, present version of pipeline).

        :param pipelineRun:
        :type 
            ~azure.mgmt.datafactory.models.pipelineRun or 
            ~azure.mgmt.datafactory.models.pipelineResource:
        :rtype dict(str, str or list):
        """
        output = {
                "run_id":None,
                "pipeline_name":None,
                "status":None,
                "message":None,
                "annotations": None,
                "parameters":pipelineRun.parameters,
                "run_start":None,
                "run_end":None,
                "adf_url": None,
                "activities":[]
            }
        if isinstance(pipelineRun, PipelineRun):
            output.update({
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
            })
        else:
            output.update({"pipeline_name":pipelineRun.name})
        
        return output

    def query_single_pipe(self, **kwargs):
        """
        Pulls out the important information in a consistent way for
        either a pipeline run (in progress or completed) or a 
        pipeline resource (not executed, present version of pipeline).

        :param str run_id: Indicates that this is an in-progress or completed
            pipeline run.  This will query the Data Factory's run activities
            and only return information about activities that failed or
            succeeded.
        :param str pipeline_name: This will query the Data Factory's pipeline
            definition and return information about all activites that are used
            in the published pipeline.
        :rtype dict(str, str or list):
        """
        if "run_id" in kwargs:
            run_id = kwargs["run_id"]
            pipe = self.adf_client.pipeline_runs.get(
                _RESOURCE_GROUP, 
                _DATA_FACTORY_NAME, 
                run_id
            )
            rpf = RunFilterParameters(
                last_updated_after=pipe.run_start,
                last_updated_before=pipe.run_end
            )
            
            pipe_activities = self.query_activities(run_id, rpf)
            output_pipe = self.extract_standard_pipeline_data(pipe)
        elif "pipeline_name" in kwargs:
            pipeline_name = kwargs["pipeline_name"]
            pipe = self.adf_client.pipelines.get(
                _RESOURCE_GROUP, 
                _DATA_FACTORY_NAME, 
                pipeline_name
            )
            output_pipe = self.extract_standard_pipeline_data(pipe)
            pipe_activities = [self.extract_standard_activities(activity) for activity in pipe.activities]
        
        output_pipe["activities"] = pipe_activities
        
        if "queue_id" in kwargs:
            output_pipe["queue_id"] = kwargs.get("queue_id")

        return output_pipe


    def query_all_pipes(self, start, end = datetime.now(), include_activities=False, latest_only=True):
        time_zone = pytz.timezone(_TIME_ZONE)

        start = time_zone.localize(datetime.strptime(start, ISO8601_FORMAT))
        end = time_zone.localize(end)

        rpf = RunFilterParameters(
            last_updated_after=start,
            last_updated_before=end,
            order_by=[RunQueryOrderBy(order_by="RunStart", order="DESC")]
        )  
        results = self.adf_client.pipeline_runs.query_by_factory(
            _RESOURCE_GROUP, 
            _DATA_FACTORY_NAME, 
            filter_parameters=rpf
        )

        output = []
        for pipe in results.value:
            if latest_only and not pipe.additional_properties.get("isLatest"):
                continue
            
            output_pipe = self.extract_standard_pipeline_data(pipe)
            if include_activities:
                rpf_activity = RunFilterParameters(
                    last_updated_after=start,
                    last_updated_before=end,
                    order_by=[RunQueryOrderBy(order_by="ActivityRunStart", order="DESC")]
                )  
                pipe_activities = self.query_activities(pipe.run_id, rpf_activity)
                output_pipe["activities"] = pipe_activities
            elif "activities" in output_pipe:
                _ = output_pipe.pop("activities")
                
            output.append(output_pipe)
        
        return output

    def execute_pipeline(self, pipeline_name, **kwargs):
        parameters = kwargs.get('parameters', None)
        issued = self.adf_client.pipelines.create_run(
            _RESOURCE_GROUP, 
            _DATA_FACTORY_NAME,
            pipeline_name,
            parameters = parameters
        )
        return issued.run_id