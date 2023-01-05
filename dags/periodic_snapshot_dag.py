import airflow
from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from constants import JAR_PATH

default_args = {
    'owner': 'Aleksandar Milanovic',
    'depends_on_past': False,
    'email': ['milanovicalex77@gmail.com'],
}

with DAG(
    dag_id='periodic_snapshot',
    default_args=default_args,
    description='A simple periodic snapshot table example',
    schedule_interval='@daily',
    start_date='2023-01-06',
    catchup=False,
    tags=['AleX77NP, periodic snapshot table example'],
) as dag:
    spark_periodic_snapshot_job = SparkSubmitOperator(
        task_id='spark_periodic_snapshot_job',
        application='/Users/aleksandar77np/Desktop/DE/FactTables/pipeline.py',
        name='periodic_snapshot',
        total_executor_cores=1,  # due to nature of sqlite in this example
        conn_id='spark_default',
        jars=JAR_PATH,
        driver_class_path=JAR_PATH
)
