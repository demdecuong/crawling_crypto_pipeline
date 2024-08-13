from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

# Define default arguments
default_args = {
    "owner": "minhnp",
    "start_date": datetime(2024, 5, 30),
    "retries": 1,
}

create_sol_table_sql = """
CREATE TABLE IF NOT EXISTS scrape.sol_stream_table (
    id SERIAL PRIMARY KEY,
    rank VARCHAR(10) NULL,
    supply NUMERIC(50 , 18) NULL,
    maxSupply NUMERIC(50 , 18) NULL,
    marketCapUsd NUMERIC(50 , 18) NULL,
    volumeUsd24Hr NUMERIC(50 , 18) NULL,
    priceUsd NUMERIC(50,18) NULL,
    changePercent24Hr NUMERIC(50,18) NULL,
    vwap24Hr NUMERIC(50,18) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

create_btc_table_sql = """
CREATE TABLE IF NOT EXISTS scrape.btc_stream_table (
    id SERIAL PRIMARY KEY,
    rank VARCHAR(10) NULL,
    supply NUMERIC(50 , 18) NULL,
    maxSupply NUMERIC(50 , 18) NULL,
    marketCapUsd NUMERIC(50 , 18) NULL,
    volumeUsd24Hr NUMERIC(50 , 18) NULL,
    priceUsd NUMERIC(50,18) NULL,
    changePercent24Hr NUMERIC(50,18) NULL,
    vwap24Hr NUMERIC(50,18) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

create_bnb_table_sql = """
CREATE TABLE IF NOT EXISTS scrape.bnb_stream_table (
    id SERIAL PRIMARY KEY,
    rank VARCHAR(10) NULL,
    supply NUMERIC(50, 18) NULL,
    maxSupply NUMERIC(50, 18) NULL,
    marketCapUsd NUMERIC(50, 18) NULL,
    volumeUsd24Hr NUMERIC(50, 18) NULL,
    priceUsd NUMERIC(50,18) NULL,
    changePercent24Hr NUMERIC(50,18) NULL,
    vwap24Hr NUMERIC(50,18) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
create_schema = """
CREATE SCHEMA IF NOT EXISTS scrape;
"""
CONN_ID = "postgres_default"
with DAG(
    dag_id="setup_postgres_table",
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={"owner": "donus", "start_date": "2021-08-15"},
) as dag:

    task0 = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id=CONN_ID,
        sql=create_schema,
    )

    task1 = PostgresOperator(
        task_id="create_bnb_stream_table",
        postgres_conn_id=CONN_ID,
        sql=create_bnb_table_sql,
    )

    task2 = PostgresOperator(
        task_id="create_btc_stream_table",
        postgres_conn_id=CONN_ID,
        sql=create_btc_table_sql,
    )
    
    task3 = PostgresOperator(
        task_id="create_sol_stream_table",
        postgres_conn_id=CONN_ID,
        sql=create_sol_table_sql,
    )

    task0 >> task1 >> task2 >> task3