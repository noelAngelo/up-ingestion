from __future__ import annotations
from typing import TYPE_CHECKING, Any, Sequence
from functools import cached_property

from airflow.utils.context import Context
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from include.hooks._minio import MinIOHook
from minio.error import S3Error


class MinioCreateBucketOperator(BaseOperator):

    def __init__(
            self,
            *,
            minio_conn_id: str = "minio_default",
            bucket_name: str | None = None,
            location: str | None = None,
            object_lock: bool | None = False,
            do_xcom_push: bool = True,
            **kwargs
    ) -> None:
        """Creates a new ``MinioCreateBucketOperator``"""
        super().__init__(**kwargs)
        self.minio_conn_id = minio_conn_id
        self.bucket_name = bucket_name
        self.location = location
        self.object_lock = object_lock
        # This variable will be used in case our task gets killed.
        self.run_id: int | None = None
        self.do_xcom_push = do_xcom_push

    def _get_hook(self) -> MinIOHook:
        return MinIOHook(
            self.minio_conn_id,
        )

    def execute(self, context: Context) -> Any:
        hook = self._get_hook()
        client = hook.get_client()
        try:
            client.make_bucket(
                bucket_name=self.bucket_name,
                location=self.location,
                object_lock=self.object_lock)
        except S3Error as e:
            if e.code == 'BucketAlreadyOwnedByYou':
                raise AirflowSkipException
