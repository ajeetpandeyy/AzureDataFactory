import os
import pyodbc

_SQL_DB_CONNECTION_STRING = os.environ.get("SQLDB_CONNECTION")


class QueueItem():

    def __init__(self, queue_id, pipeline_name, pipeline_status):
        self.queue_id = queue_id
        self.pipeline_name = pipeline_name
        self.pipeline_status = pipeline_status


def query_queue(filters = None):
    """ 
    Interface for selecting the Issued and InProgress Pipelines

    :param list(str) filters: 
        Key value pairs to be passed into a where condition. Each
        filter is joined by an AND clause.
    :return: 
        The pipelines that are in-progress and queued defined in
        a dictionary.
    :rtype: dict(str, ~queue.QueueItem)
    """

    conn = pyodbc.connect(_SQL_DB_CONNECTION_STRING)
    cursor = conn.cursor()

    query = "SELECT Pipeline_Key, Pipeline_Name, Pipeline_Status FROM dbo.DataFactory_Queue"
    if filters:
        filter_text = ' AND '.join(filters)
        query = query + " WHERE " + filter_text

    query = query + ";"

    cursor.execute(query)

    output = {"InProgress": [], "Issued":[], "Complete":[]}
    for row in cursor:
        output[row[2]].append(
            QueueItem(
                queue_id = row[0],
                pipeline_name = row[1],
                pipeline_status = row[2]
            )
        )

    cursor.close()
    conn.close()
    return output

def update_queue(updates):
    """ 
    Interface for updating the Issued Pipelines

    :param list(dict(str, str or list)) updates: 
        Contains unique identifier to issue an update query against and
        a list of id fields to update a status field.  The dictionary
        must be {'id':'field', 'updated':value}.  If passing strings, 
        ensure they are already single quoted when passing into this function.
    
    :rtype list(tuple)
    """

    conn = pyodbc.connect(_SQL_DB_CONNECTION_STRING)
    cursor = conn.cursor()

    query = "UPDATE dbo.DataFactory_Queue SET Pipeline_Status = 'InProgress' WHERE {} = {};".format(
        updates["id"], updates["updated"]
    )
    
    cursor.execute(query)
    conn.commit()
    
    cursor.close()
    conn.close()
    return 1