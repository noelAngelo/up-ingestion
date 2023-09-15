# import the operator to inherit from
from airflow.models.baseoperator import BaseOperator


# define the class inheriting from an existing hook class
class UpOperator(BaseOperator):
    """
    Simple example operator that logs one parameter and returns a string saying hi.
    :param my_parameter: (required) parameter taking any input.
    """
    pass
