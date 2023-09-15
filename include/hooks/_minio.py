# import the hook to inherit from
from airflow.hooks.base import BaseHook
from minio import Minio
from urllib.parse import urlparse


# define the class inheriting from an existing hook class
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
        # (optional) call the '.get_client()' method upon initialization
        self.get_client()

    def get_conn(self):
        """Function that initiates a new connection to MinIO."""
        # retrieve the passed connection id
        conn_id = getattr(self, self.conn_name_attr)
        # get the connection object from the Airflow connection
        conn = self.get_connection(conn_id)

        return conn

    def get_client(self):
        """Function that initiates a MinIO client"""
        # retrieve the passed connection id
        conn_id = getattr(self, self.conn_name_attr)
        # get the connection object from the Airflow connection
        conn = self.get_connection(conn_id)
        # build minio client
        host = urlparse(conn.host).hostname
        client = Minio(endpoint=f'{host}:{conn.port}',
                       access_key=conn.login,
                       secret_key=conn.password,
                       secure=False)
        return client
