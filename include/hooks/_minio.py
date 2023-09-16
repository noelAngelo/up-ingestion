from airflow.hooks.base import BaseHook
from minio import Minio
from urllib.parse import urlparse


class MinIOHook(BaseHook):
    """
    Interact with MinIO API.
    :param my_conn_id: ID of the connection to MinIO
    """

    # provide the name of the parameter which receives the connection id
    conn_name_attr = "minio_conn_id"
    # provide a default connection id
    default_conn_name = "minio_default"
    # provide the connection type
    conn_type = "http"
    # provide the name of the hook
    hook_name = "Minio"

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(
            self, minio_conn_id: str = default_conn_name, *args, **kwargs
    ) -> None:
        # initialize the parent hook
        super().__init__(*args, **kwargs)
        # assign class variables
        self.minio_conn_id = minio_conn_id
        self.minio_conn = self.get_connection(minio_conn_id)

    @staticmethod
    def _parse_host(host: str):
        """
        The purpose of this function is to be robust to improper connections
        settings provided by users, specifically in the host field.

        For example -- when users supply ``https://xx.minio.com`` as the
        host, we must strip out the protocol to get the host.::

            h = MinioHook()
            assert h._parse_host('https://xx.minio.com') == \
                'minio.com'

        In the case where users supply the correct ``xx.minio.com`` as the
        host, this function is a no-op.::

            assert h._parse_host('xx.minio.com') == 'xx.minio.com'

        """
        urlparse_host = urlparse(host).hostname
        if urlparse_host:
            # In this case, host = https://xx.minio.com
            return urlparse_host
        else:
            # In this case, host = xx.minio.com
            return host

    def get_client(self):
        """Function that initiates a MinIO client"""
        # retrieve the passed connection id
        conn_id = getattr(self, self.conn_name_attr)
        # get the connection object from the Airflow connection
        conn = self.get_connection(conn_id)
        # build minio client
        host = self._parse_host(conn.host)
        client = Minio(endpoint=f'{host}:{conn.port}',
                       access_key=conn.login,
                       secret_key=conn.password,
                       secure=False)
        return client

