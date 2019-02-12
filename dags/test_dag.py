from __future__ import print_function

from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'esoboliev',
    'start_date': datetime.utcnow(),
    'schedule_interval': '@once'
}

dag = DAG(dag_id="TestDag",
          default_args=default_args)

task0 = BashOperator(task_id='set_user',
                     bash_command="./get_user.sh",
                     xcom_push=True,
                     dag=dag)

task1 = BashOperator(task_id='set_variable',
                     bash_command='echo {{ ti.xcom_pull("set_user") }}',
                     dag=dag)

task2 = BashOperator(task_id='show_variable_inside_bash_script',
                     bash_command='./show_user.sh',
                     context=True,
                     dag=dag)

task0 >> task1 >> task2
