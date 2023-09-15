# import the hook to inherit from
from airflow.hooks.base import BaseHook


# define the class inheriting from an existing hook class
class UpHook(BaseHook):
    """
    Interact with Up Bank API.
    :param my_conn_id: ID of the connection to Up Bank
    """

    # provide the name of the parameter which receives the connection id
    conn_name_attr = "up_conn_id"
    # provide a default connection id
    default_conn_name = "up_default"
    # provide the connection type
    conn_type = "http"
    # provide the name of the hook
    hook_name = "UpHook"

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(
            self, my_conn_id: str = default_conn_name, *args, **kwargs
    ) -> None:
        # initialize the parent hook
        super().__init__(*args, **kwargs)
        # assign class variables
        self.my_conn_id = my_conn_id
        # (optional) call the '.get_conn()' method upon initialization
        self.get_conn()

    def get_conn(self):
        """Function that initiates a new connection to your external tool."""
        # retrieve the passed connection id
        conn_id = getattr(self, self.conn_name_attr)
        # get the connection object from the Airflow connection
        conn = self.get_connection(conn_id)

        return conn

