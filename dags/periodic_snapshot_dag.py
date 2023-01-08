from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from constants import APPLICATION_PATH, JAR_PATH

from datetime import timedelta
import pendulum


def intro():
    print('Preparing to insert weekly data into periodic snapshot table...')


default_args = {
    'owner': 'Aleksandar Milanovic',
    'depends_on_past': False,
    'email': ['milanovicalex77@gmail.com'],
}

with DAG(
        dag_id='periodic_snapshot_dag',
        default_args=default_args,
        description='A simple periodic snapshot table example',
        schedule_interval=timedelta(days=7),
        start_date=pendulum.datetime(2022, 2, 6, tz='UTC'),
        catchup=False,
        tags=['AleX77NP, periodic snapshot table example'],
) as dag:
    intro_job = PythonOperator(
        task_id='intro_job',
        python_callable=intro,
    )
    spark_periodic_snapshot_job = SparkSubmitOperator(
        task_id='spark_periodic_snapshot_job',
        application=f'{APPLICATION_PATH}/pipeline.py',
        name='periodic_snapshot',
        executor_cores=1,
        total_executor_cores=1, # due to nature of sqlite
        conn_id='spark_local',
        jars=JAR_PATH,
        driver_class_path=JAR_PATH
    )

    intro_job >> spark_periodic_snapshot_job
