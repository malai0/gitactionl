from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pyspark_etl_internal_df',
    default_args=default_args,
    description='PySpark ETL with internal DataFrame and Airflow',
    schedule_interval='@daily',
    catchup=False,
)

output_path = "/path/to/output/parquet_data"

run_etl = BashOperator(
    task_id='run_pyspark_etl',
    bash_command=f"spark-submit /path/to/etl_pyspark.py {output_path}",
    dag=dag,
)
