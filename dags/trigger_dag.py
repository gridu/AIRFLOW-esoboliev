from datetime import datetime

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

default_args = {
    'owner': 'esoboliev',
    'start_date': datetime.utcnow(),
    'schedule_interval': '@once',
    'provide_context': True
}

dag = DAG('RemoveRunFile', default_args=default_args)

path = Variable.get('path_to_file', default_var='/tmp/test_file.txt')

file_sensor_oper = FileSensor(task_id='sensor_wait_run_file',
                              run_as_user='esoboliev',
                              filepath=path,
                              fs_conn_id='fs_default',
                              dag=dag)

trigger_dag_oper = TriggerDagRunOperator(task_id='trigger_dag',
                                         trigger_dag_id='dag_id_1',
                                         dag=dag)


def pull_function(**kwargs):
    kwargs['ti'].xcom_push(key='result_run_info',
                           value='{} ended'.format(kwargs.get('run_id')))
    print(kwargs['ti'].xcom_pull(key='result_run_info'))
    print(kwargs)


def generate_sub_dag(parent_dag_name, child_dag_name):
    inner_dag = DAG(dag_id="{}.{}".format(parent_dag_name, child_dag_name),
                    default_args=default_args)

    print_result = PythonOperator(task_id='print_results',
                                  python_callable=pull_function,
                                  default_args=default_args,
                                  dag=inner_dag)

    remove_file_oper = BashOperator(task_id='remove_run_file',
                                    run_as_user='esoboliev',
                                    bash_command='rm {}'.format(path),
                                    default_args=default_args,
                                    dag=inner_dag)

    create_timestamp = BashOperator(task_id='create_timestamp',
                                    run_as_user='esoboliev',
                                    bash_command='touch /tmp/finished_{}'.format(
                                        '{{ ts_nodash }}'),
                                    default_args=default_args,
                                    dag=inner_dag)

    print_result >> remove_file_oper >> create_timestamp

    return inner_dag


sub_dag_oper = SubDagOperator(
    task_id='process_results_sub_dag',
    subdag=generate_sub_dag(dag.dag_id,
                            'process_results_sub_dag'),
    default_args=default_args,
    dag=dag)

file_sensor_oper >> trigger_dag_oper >> sub_dag_oper
