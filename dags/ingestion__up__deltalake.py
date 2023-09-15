import json
import requests
from minio import Minio
from pendulum import datetime
from airflow.decorators import (
    dag,
    task
)  # DAG and task decorators for interfacing with the TaskFlow API
from airflow.exceptions import AirflowSkipException


@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    schedule='@daily',
    # This DAG is set to run for the first time on January 1, 2023. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on the schedule
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 1
    },
    tags=["ingestion"]
)
def ingestion__up__deltalake():
    """
    ### Basic batch ingestion of API responses
    This is a simple ingestion data pipeline for ingesting the responses of an API call
    in batches. It demonstrates the use of TaskFlow API for collecting responses from Up
    and storing its results in an object store or delta lake.
    """

    @task()
    def create_bucket():
        """
        #### setup task
        A simple setup task to build the bucket for our delta lake
        """
        bucket_name = 'sources-prod-up'
        client = Minio("192.168.0.21:9000", access_key='minio', secret_key='minio123', secure=False)
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
        else:
            print(f"Bucket <{bucket_name}> already exists")
            raise AirflowSkipException

    create_bucket().as_setup()


ingestion__up__deltalake()
