from __future__ import annotations
from typing import TYPE_CHECKING, Any, Sequence

from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator
from include.hooks.up import UpHook


class UpOperator(BaseOperator):

    def __init__(
            self,
            *,
            up_conn_id: str = "up_default",
            method: str | None = None,
            endpoint: str | None = None,
            query_params: dict | None = None,
            **kwargs
    ) -> None:
        """
        Creates a new UpOperator

        :param up_conn_id:
        :param method:
        :param endpoint:
        :param query_params:
        :param kwargs:
        """
        super().__init__(**kwargs)
        self.up_conn_id = up_conn_id
        self.endpoint = endpoint
        self.method = method
        self.query_params = query_params

    def _get_hook(self) -> UpHook:
        return UpHook(
            self.up_conn_id
        )

    def execute(self, context: Context) -> dict:
        hook = self._get_hook()
        return hook.do_api_call(endpoint_info=(self.method, self.endpoint), json=self.query_params)
