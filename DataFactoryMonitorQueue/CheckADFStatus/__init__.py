import logging
import json
from collections import Counter
from .queue import query_queue, update_queue
from .adf import ISO8601_FORMAT, ADFManagement
from datetime import datetime, timedelta

import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python Timer trigger function processed a request.')

    # TODO: Add Filters to the queue query
    current_queue_status = query_queue()

    if len(current_queue_status["Issued"]) == 0:
        logging.info("There were no Pipelines with a status of Issued.  Exiting function.")
        return None
    
    logging.info(f'There are {len(current_queue_status["InProgress"])} pipelines returned from the queue.')
    # Look at the in progress queues and find all the linked services in use

    adf = ADFManagement()
    # TODO: Define critical linked services and their threshold (database call?)
    critical_services =  []

    linked_services_in_use = Counter()
    # Filter down to the InProgress pipes
    active_pipes = current_queue_status["InProgress"]
    # Query the Data Factory for all Linked Services of the activites or Datasets
    pipe_details = [adf.query_single_pipe(pipeline_name = pipe.pipeline_name) for pipe in active_pipes]

    # Iterate over the details of each pipeline
    for pipe in pipe_details:
        for activity in pipe["activities"]:
            # TODO: Filter for critical services only
            linked_services_in_use.update(
                Counter(set(activity["linked_services"]))
            )
    
    linked_services_in_use_string = json.dumps(dict(linked_services_in_use))

    logging.info(f"The critical services in use: {linked_services_in_use_string}")

    # TODO: Determine logic preventing execution
    issued_pipes = current_queue_status["Issued"]
    issued_details = [adf.query_single_pipe(pipeline_name = pipe.pipeline_name, queue_id = pipe.queue_id) for pipe in issued_pipes]
    logging.info(f"There are {len(issued_details)} pipelines issued.")
    for pipe in issued_details:
        working_pipeline_name = pipe["pipeline_name"]
        issued_run_id = adf.execute_pipeline(working_pipeline_name)
        logging.info(f"Pipeline {working_pipeline_name} was issued with {issued_run_id} run id")
        
        pipe_queue_id = pipe["queue_id"]
        _ = update_queue({"id":"Pipeline_Key","updated":pipe_queue_id})
        logging.info(f"Successfully updated queue with id: {pipe_queue_id}.")

    #return func.HttpResponse(f"{linked_services_in_use_string}")
 