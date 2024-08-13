import sys, types

m = types.ModuleType('kafka.vendor.six.moves', 'Mock module')
setattr(m, 'range', range)
sys.modules['kafka.vendor.six.moves'] = m

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from src.kafka_producer import send_data

args = {"owner": "minhnp", "start_date": datetime(2024, 5, 29), "provide_context": True}

dag = DAG(
    dag_id="kafka_producer_dag",
    default_args=args,
    schedule_interval=timedelta(seconds=45),
    catchup=False,
)

task = PythonOperator(task_id="send_data", python_callable=send_data, dag=dag)
