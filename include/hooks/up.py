#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Up Bank hook.

This hook enable the communication between the Up platform. Internally the
operators talk to the ``https://api.up.com.au/api/v1`` host.
"""

from urllib.parse import urlparse

import requests
from requests import PreparedRequest, exceptions as requests_exceptions
from requests.auth import AuthBase

from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

ACCOUNTS__ENDPOINTS = ("GET", "api/v1/accounts")
CATEGORIES__ENDPOINTS = ("GET", "api/v1/categories")
TAGS__ENDPOINTS = ("GET", "api/v1/tags")
TRANSACTIONS__ENDPOINTS = ("GET", "api/v1/transactions")
UTILITY__ENDPOINTS = ("GET", "api/v1/util/ping")
USER_AGENT_HEADER = {'user-agent': f'airflow-{__version__}'}


# define the class inheriting from an existing hook class
class UpHook(BaseHook):
    """
    Interact with Up Bank API.
    :param up_conn_id: ID of the connection to Up Bank
    """

    # provide the name of the parameter which receives the connection id
    conn_name_attr = "up_conn_id"
    # provide a default connection id
    default_conn_name = "up_default"
    # provide the connection type
    conn_type = "http"
    # provide the name of the hook
    hook_name = "Up"

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(
            self, up_conn_id: str = default_conn_name, *args, **kwargs
    ) -> None:
        # initialize the parent hook
        super().__init__(*args, **kwargs)
        # assign class variables
        self.up_conn_id = up_conn_id
        self.up_conn = self.get_connection(up_conn_id)

    def do_api_call(self, endpoint_info: tuple[str, str], json: dict) -> dict:
        """
        Utility function to perform an API call

        :param endpoint_info: Tuple of method and endpoint
        :type endpoint_info: tuple[string, string]
        :param json: Parameters for this API call
        :return: If the api call returns a OK status code,
            this function returns the response in JSON. Otherwise,
            we throw an AirflowException
        """
        method, endpoint = endpoint_info
        self.log.info('Using token auth. ')
        auth = _TokenAuth(self.up_conn.password)
        url = '{host}/api/v1/{endpoint}'.format(host=self.up_conn.host,
                                                endpoint=endpoint)
        if method == 'GET':
            request_func = requests.get
        elif method == 'POST':
            request_func = requests.post
        elif method == 'PATCH':
            request_func = requests.patch
        else:
            raise AirflowException('Unexpected HTTP Method: ' + method)

        self.log.info(f'Requesting `{method}` method on `{endpoint}` endpoint..')
        self.log.info(f'Request URL: {url}')
        response = request_func(
            url,
            json=json if method in ('POST', 'PATCH') else None,
            params=json if method == 'GET' else None,
            auth=auth
        )
        response.raise_for_status()
        return response.json()


class _TokenAuth(AuthBase):
    """
    Helper class for requests Auth field. AuthBase requires you to implement the __call__
    magic function.
    """

    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        r.headers['Authorization'] = 'Bearer ' + self.token
        return r
