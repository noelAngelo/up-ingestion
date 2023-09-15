from pendulum import datetime
from airflow.decorators import (
    dag,
    task
)  # DAG and task decorators for interfacing with the TaskFlow API
from airflow.operators.empty import EmptyOperator
from include.operators._minio import MinioCreateBucketOperator


@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    schedule='@daily',
    # This DAG is set to run for the first time on January 1, 2023. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on the schedule
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "max_active_runs": 1
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

    # include Ops
    create_bucket_op = MinioCreateBucketOperator(
        task_id='create_bucket',
        minio_conn_id='minio_default',
        bucket_name='sources-prod-up'
    )
    start = EmptyOperator(task_id='start')

    # define workflow
    start >> create_bucket_op.as_setup()


ingestion__up__deltalake()
