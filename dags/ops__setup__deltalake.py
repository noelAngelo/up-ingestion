from airflow.decorators import (dag, task)
from airflow.providers.trino.operators.trino import TrinoOperator
from include.operators._minio import MinioCreateBucketOperator
from datetime import datetime

# GLOBALS
DELTA_CATALOG = 'delta'
DELTA_BUCKET = 'deltalake'


@dag(
    schedule="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "max_active_runs": 1
    },
    tags=["operations"]
)
def operations__setup__deltalake():
    create_delta_bucket = MinioCreateBucketOperator(
        task_id='create_delta_bucket',
        minio_conn_id='minio_default',
        bucket_name=DELTA_BUCKET
    )

    create_up_schema = TrinoOperator(
        task_id='create_deltalake_schema',
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS {DELTA_CATALOG}.up
        WITH (location = 's3a://{DELTA_BUCKET}/up');
        """
    )

    # define workflow
    create_delta_bucket >> create_up_schema


operations__setup__deltalake()
