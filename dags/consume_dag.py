import sys, types

m = types.ModuleType('kafka.vendor.six.moves', 'Mock module')
setattr(m, 'range', range)
sys.modules['kafka.vendor.six.moves'] = m

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from src.kafka_consumer import consume_kafka_topic

args = {"owner": "minhnp", "start_date": datetime(2024, 5, 29), "provide_context": True}

dag = DAG(
    dag_id="kafka_consumer_dag",
    default_args=args,
    schedule_interval=timedelta(seconds=45),
    catchup=False,
)
SCHEMA = 'scrape'
TASKS = {'sol-topic':None,'bnb-topic':None,'btc-topic':None}
TOPIC_TABLENAME = {'sol-topic':'sol_stream_table','bnb-topic':'bnb_stream_table','btc-topic':'btc_stream_table'}
USERNAME = 'airflow'
PASSWORD = 'airflow'
DATABASE = 'airflow'
for topic,table_name in TOPIC_TABLENAME.items():
    TASKS[topic] = PythonOperator(task_id=f"consume_{topic}", python_callable=consume_kafka_topic,
                        op_kwargs={
                            'topic':topic, 
                            'database':DATABASE, 
                            'username':USERNAME, 
                            'password':PASSWORD, 
                            'tablename': f'{SCHEMA}.{table_name}'
                            },
                        dag=dag)

TASKS['sol-topic']
TASKS['bnb-topic']
TASKS['btc-topic']