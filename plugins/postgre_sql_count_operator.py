from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils import apply_defaults


class PostgreSQLCountRows(BaseOperator):

    @apply_defaults
    def __init__(self, table_name, postgres_conn_id, *args, **kwargs):
        """
        :param table_name: table name
        """
        self.table_name = table_name
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        super(PostgreSQLCountRows, self).__init__(*args, **kwargs)

    def execute(self, context):
        result = self.hook.get_first(
            sql="SELECT COUNT(*) FROM {};".format(self.table_name))
        return result


class PostgreSQLCustomOperatorsPlugin(AirflowPlugin):
    name = "postgres_custom"
    operators = [PostgreSQLCountRows]
