# q-etl

This is a distributed job processing system for offline jobs.

Getting Started
---------------
Recommend to use docker for development setup
```
chmod 777 setup.sh
./setup.sh
python worker_pull_data.py & # Worker for job execution
python worker_update_db.py & # Worker for db updation
```

Testing
-------
We can update the `scheduler.py` with the message and run the script to trigger the corresponding job

Scaling
-------
We can clone file `worker_pull_data.py` and `worker_update_db.py` with different names and run them to add more workers to the system

Note: The scripts can be placed under different machines but `config.py` should be updated with details of Kafka and Monitoring API

Monitoring
-----------
The system can be monitored by using API endpoint

GET `http://localhost/worker/<worker_id>`

Will return 
* name of the worker
* host machine ip
* worker start time
* total messages processed
* total successfully processed 



Components
----------
* `Apache Kafka` - Message Broker
* `FastAPI` - API Powered Monitoring System
* `Uvicorn` - HTTP Server for the API
* `Sqlite` - Backend database for the monitoring system

Kafka Topics
------------
1. `job` - queue where job triggers are pushed
2. `db`  - queue where the jobs pushes the downloaded data
3. `error` - queue where error messages are captured

Workflow Pipeline
-----------------
1. Trigger to a job -> Can be any external system that can post to a Kafka topic
2. Pipeline executes the relelvant job based on the trigger and returns the data back 
3. The pipeline pushes data to `db` queue for db updation
4. Data is updated from the `db` queue to the backend system.
5. Presently the backend is `AWS S3`. It can be easily decoupled and changed to other backends (Credentials should be updated in `q_etl.etl` line 41)


Sample Message Formats
----------------------

Topic -> job

```
{
    "method": "ingest_covid_data",
    "params": {}
}
``` 

Topic -> db
```
[
    {"table": "TableA", "refresh": True, "data": [list of dictionary of rows]},
    {"table": "TableB", "refresh": True, "data": [list of dictionary of rows]},
]

```

Interfaces
----------
1. External system triggers a job by posting a message to the `job` topic
2. The pipeline takes care of the uplifting of data updation returned by the method, provided the below conditions are met

Criterias for a job
-------------------
1. The scripts can be placed under `scripts` package
2. The callable function should be imported and registered in the module `q_etl/register`
3. The callable function should be awaitable or asynchronous
4. The callable function should return output in the below format
. 
```
    def mycallable(*args, **kwargs) -> Tuple[List[Dict[str, Any]], Any]

    Example call from  the Pipeline:
        method_response, method_error = await method(**params)
    
    Sample value for method response:
        method_response = [
            {"table": "Table1", "refresh": True, "data": [{}]},
            {"table": "Table2", "refresh": True, "data": [{}]},
            {"table": "Table3", "refresh": True, "data": [{}]}
        ]
```
