from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import BranchPythonOperator

config = {
	# TODO remember to change dag_id when changing table_name
	'dag_id_1': {'schedule_interval': None, "start_date": datetime(2021, 1, 1), 'table_name': 'table_name_1'},
	'dag_id_2': {'schedule_interval': None, "start_date": datetime(2021, 1, 1), 'table_name': 'table_name_2'},
	'dag_id_3': {'schedule_interval': None, "start_date": datetime(2021, 1, 1), 'table_name': 'table_name_3'}
}

def DB_task(dag_id, database):
	print(f"{dag_id} start processing tables in database: {database}")

def check_table_exists():
	if True:
		return 'insert_new_row'
	return 'create_table'

def push_to_xcom(**kwargs):
	kwargs['ti'].xcom_push(key='msg', value='{{ run_id }} ended')

for dict in config:
	with DAG(dict, default_args=config[dict]) as dag:
		print_task = PythonOperator(task_id='print_process_start', python_callable=DB_task, op_kwargs={'dag_id':dict, 'database': 'db_test'})
		get_user = BashOperator(task_id='get_current_user', bash_command='whoami')
		branch_task = BranchPythonOperator(task_id='check_table_exists', python_callable=check_table_exists)
		create_table_task = DummyOperator(task_id='create_table')
		insert_task = DummyOperator(task_id='insert_new_row', trigger_rule='none_failed')
		query_task = DummyOperator(task_id='query_the_table')
		push_to_xcom_done = PythonOperator(task_id='push_to_xcom', python_callable=push_to_xcom, provide_context=True)


		dag >> print_task >> get_user >> branch_task >> [create_table_task, insert_task]
		create_table_task >> insert_task
		insert_task >> query_task
		[insert_task, query_task] >> push_to_xcom_done
		globals()[dict] = dag

