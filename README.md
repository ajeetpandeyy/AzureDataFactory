# AzureDataFactory
A collection of resources, projects, and snippets for use in Azure Data Factory

## Projects

* **DataFactoryMonitor**: A small Flask web app that automatically refreshes and pulls data on your data factory pipelines. If you're monitoring a page and want to see your pipelines up date in real-time, this can help (though watch out for the costs of reading pipeline runs and activity runs).
* **DataFactoryMonitorQueue**: An Azure Function that runs Data Factory pipeline jobs based on a control table in a SQL DB.  Uses a Timer Trigger to look for "issued" job requests and then runs them if your Linked Services aren't already being used by another job (to avoid over burdening a given linked service).
