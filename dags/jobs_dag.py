from __future__ import print_function

import time
import uuid
from datetime import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

from plugins.postgre_sql_count_operator import PostgreSQLCountRows

DB_SCHEMA = "airflow_db"
DB_TABLE_NAME = "user_executions"

CHECK_TABLE_EXISTS_QUERY = """
   SELECT EXISTS (
       SELECT 1
       FROM   information_schema.tables 
       WHERE  table_schema = '{}'
       AND    table_name = '{}'
   );
"""

CREATE_TABLE_QUERY = """
CREATE TABLE user_executions(
    id VARCHAR (50) UNIQUE PRIMARY KEY,
    username VARCHAR (50) NOT NULL,
    timestamp TIMESTAMP NULL
);
"""
INSERT_TABLE_ROW_QUERY = """
    INSERT INTO user_executions VALUES
        ('{}', '{}', '{}');
"""

SELECT_ALL_QUERY = """
   SELECT * FROM {};
"""

config = {
    'dag_id_1': {'schedule_interval': "@once",
                 'start_date': datetime.utcnow(),
                 'table_name': 'table_name_1',
                 'owner': 'esoboliev'},
    'dag_id_2': {'schedule_interval': "@once",
                 'start_date': datetime.utcnow(),
                 'table_name': 'table_name_1',
                 'owner': 'esoboliev'},
    'dag_id_3': {'schedule_interval': "@once",
                 'start_date': datetime.utcnow(),
                 'table_name': 'table_name_1',
                 'owner': 'esoboliev'}
}


def log_information(**context):
    print("{} start processing tables in database: {}"
          .format(context.get('ti.dag_id'), DB_SCHEMA))


def check_table_exist():
    query = PostgresHook(postgres_conn_id='postgres_connection') \
        .get_first(sql=CHECK_TABLE_EXISTS_QUERY.format(DB_SCHEMA, DB_TABLE_NAME))
    print(query)
    if query[0] is None:
        raise ValueError("Table {} does not exist".format(DB_TABLE_NAME))
    return 'dummy_task'


def generate_dags():
    dags = []
    for dag_id, values in config.iteritems():
        dag = DAG(dag_id=dag_id,
                  default_args=values)

        task1 = PythonOperator(task_id='log_information',
                               provide_context=True,
                               python_callable=log_information,
                               start_date=datetime.utcnow(),
                               dag=dag)

        task1_2 = BashOperator(task_id='get_current_user',
                               bash_command='echo $USER',
                               xcom_push=True,
                               dag=dag)

        condition = BranchPythonOperator(task_id='check_table_exist',
                                         python_callable=check_table_exist,
                                         dag=dag)

        condition_inner_1 = PostgresOperator(task_id='create_table',
                                             postgres_conn_id='postgres_connection',
                                             trigger_rule=TriggerRule.ALL_FAILED,
                                             sql=CREATE_TABLE_QUERY,
                                             dag=dag)

        condition_inner_2 = DummyOperator(task_id='dummy_task',
                                          trigger_rule=TriggerRule.ALL_DONE,
                                          dag=dag)

        task2 = PostgresOperator(task_id='insert_row',
                                 trigger_rule=TriggerRule.ONE_SUCCESS,
                                 postgres_conn_id='postgres_connection',
                                 sql=INSERT_TABLE_ROW_QUERY
                                 .format(uuid.uuid4(),
                                         '{{ ti.xcom_pull("get_current_user") }}',
                                         time.strftime("%Y-%m-%d %H:%M:%S",
                                                       time.gmtime())),
                                 dag=dag)

        task3 = PostgreSQLCountRows(task_id='query_the_table',
                                    table_name=DB_TABLE_NAME,
                                    postgres_conn_id='postgres_connection',
                                    dag=dag)

        task1.set_downstream(task1_2)
        condition.set_upstream(task1_2)
        condition_inner_1.set_upstream(condition)
        condition_inner_2.set_upstream(condition)
        task2.set_upstream(condition_inner_1)
        task2.set_upstream(condition_inner_2)
        task3.set_upstream(task2)

        dags.append(dag)
    return dags


for dag in generate_dags():
    globals()[dag.dag_id] = dag
