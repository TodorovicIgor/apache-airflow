from airflow.contrib.sensors.file_sensor import FileSensor
# from custom_file_sensor.py import FileSensor
from airflow import DAG
from airflow.models import Variable
from airflow.operators import TriggerDagRunOperator, BashOperator, PythonOperator
from airflow.operators.sensors import ExternalTaskSensor
import datetime
from datetime import timedelta
from airflow.operators.subdag_operator import SubDagOperator

def get_from_xcom(**kwargs):
	print(kwargs['task_instance'].xcom_pull(key='msg'))


def get_subdag(start_date):
	with DAG('file_sensor_dag.process_results_subdag', schedule_interval=None, start_date=start_date) as dag:
		# sensor_triggered_dag = ExternalTaskSensor(task_id='sensor_triggered_dag', external_dag_id='file_sensor_dag', external_task_id='None', execution_delta=timedelta(minutes=5), dag=dag)
		# sensor_triggered_dag = ExternalTaskSensor(task_id='sensor_triggered_dag', external_dag_id='file_sensor_dag', external_task_id='None', execution_date=ed, dag=dag)
		print_res = PythonOperator(task_id='print_res', python_callable=get_from_xcom, provide_context=True)
		remove_op = BashOperator(task_id='remove_run_file', bash_command='rm /tmp/run.txt')
		create_ts = BashOperator(task_id='create_ts', bash_command="touch finished_{{ ts_nodash }}")
		dag >> print_res >> remove_op >> create_ts
	return dag

'''
def set_execution_date(**kwargs):
	print('TEST')
	Variable.set('ed', kwargs['execution_date'])
'''

with DAG("file_sensor_dag", schedule_interval="@once", start_date=datetime.datetime(2021, 1, 20)) as dag:
	filepath = Variable.get('filepath', default_var='/tmp/run.txt')
	# set_execution_date = PythonOperator(task_id='set_execution_date', python_callable=set_execution_date, provide_context=True)
	sensor = FileSensor(task_id="wait_run_file", poke_interval=30, filepath=filepath)
	trigger = TriggerDagRunOperator(task_id='trigger_dag', trigger_dag_id="dag_id_1")
	process_results_subdag = SubDagOperator(
		subdag=get_subdag(datetime.datetime(2021, 1, 20)),
		task_id='process_results_subdag'
	)

	dag >> sensor >> trigger >> process_results_subdag
