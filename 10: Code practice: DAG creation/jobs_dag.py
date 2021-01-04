from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

config = {
	'dag_id_1': {'schedule_interval': None, "start_date": datetime(2021, 1, 1)},
	'dag_id_2': {'schedule_interval': None, "start_date": datetime(2021, 1, 1)},
	'dag_id_3': {'schedule_interval': None, "start_date": datetime(2021, 1, 1)}
}

for dict in config:
	with DAG(dict, default_args=config[dict]) as dag:
		task1 = DummyOperator(task_id='t1')
		dag >> task1

