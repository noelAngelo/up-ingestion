from io import BytesIO
from datetime import datetime
from airflow.decorators import (
    dag,
    task
)  # DAG and task decorators for interfacing with the TaskFlow API
from airflow.models.baseoperator import chain
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from include.hooks.up import UpHook
from include.hooks._minio import MinIOHook
from include.operators._minio import MinioCreateBucketOperator
from include.operators.up import UpOperator

# GLOBALS
MINIO_SOURCE_BUCKET_NAME = 'sources-prod-up'
UP_ENDPOINTS = ['accounts', 'categories', 'tags']

@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    schedule='@once',
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

    collect_tasks = []
    ingest_tasks = []
    for e in UP_ENDPOINTS:
        @task(trigger_rule='none_failed', task_id=f'collect_up_{e}')
        def up_to_minio(endpoint: str, query_params: dict = None):
            # instantiate hooks
            minio_hook = MinIOHook(minio_conn_id='minio_default')
            minio_client = minio_hook.get_client()
            up_hook = UpHook(up_conn_id='up_default')

            # collect data from endpoint
            response = up_hook.do_api_call(endpoint_info=('GET', endpoint), json=query_params)

            # store data
            date_format = "%a, %d %b %Y %H:%M:%S GMT"
            response_date = response.headers['Date']
            dt = datetime.strptime(response_date, date_format)
            filename = f'{dt.year}{dt.month}{dt.day}{dt.hour}{dt.minute}{dt.second}{dt.microsecond}.json'

            minio_client.put_object(
                bucket_name=MINIO_SOURCE_BUCKET_NAME,
                object_name=f'{endpoint}/{dt.year}-{dt.month}/{filename}',
                length=len(response.content),
                data=BytesIO(response.content),
                content_type=response.headers['Content-Type']
            )

        collect_tasks.append(up_to_minio(e))
        ingest_tasks.append(SparkSubmitOperator(
            task_id=f'ingest_up_{e}',
            application="${SPARK_HOME}/examples/src/main/python/pi.py"
        ))

    # include Ops
    create_bucket_op = MinioCreateBucketOperator(
        task_id='create_bucket',
        minio_conn_id='minio_default',
        bucket_name=MINIO_SOURCE_BUCKET_NAME
    )

    check_conn_op = UpOperator(
        task_id='check_up_conn',
        method='GET',
        endpoint='util/ping',
        up_conn_id='up_default',
    )

    # define workflow
    chain(create_bucket_op, check_conn_op, collect_tasks, ingest_tasks)


ingestion__up__deltalake()
