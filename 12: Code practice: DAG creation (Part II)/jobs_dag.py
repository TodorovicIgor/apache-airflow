from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

config = {
	# TODO remember to change dag_id when changing table_name
	'dag_id_1': {'schedule_interval': None, "start_date": datetime(2021, 1, 1), 'table_name': 'table_name_1'},
	'dag_id_2': {'schedule_interval': None, "start_date": datetime(2021, 1, 1), 'table_name': 'table_name_2'},
	'dag_id_3': {'schedule_interval': None, "start_date": datetime(2021, 1, 1), 'table_name': 'table_name_3'}
}

def DB_task(dag_id, database):
	print(f"{dag_id} start processing tables in database: {database}")

for dict in config:
	with DAG(dict, default_args=config[dict]) as dag:
		task1 = PythonOperator(task_id='print_info', python_callable=DB_task, op_kwargs={'dag_id':dict, 'database': 'db_test'})
		task2 = DummyOperator(task_id='insert_new_row')
		task3 = DummyOperator(task_id='query_the_table')
		dag >> task1 >> task2 >> task3

