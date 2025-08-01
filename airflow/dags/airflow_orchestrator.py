import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

sys.path.append('/opt/airflow/api_call')

def safe_main_callable():
    from data_insert import main
    return main()


def export_callable():
    from csv_data_load import csv_export
    return csv_export()

default_args= {
    'description' : 'A DAG to orchestrate data',
    'start_date' : datetime(2025,7,10),
    'catchup' : False
}


dag = DAG(
    dag_id='map-restaurants-orchestrator',
    default_args=default_args,
    schedule=timedelta(minutes=30)
)


with dag:
    task1 = PythonOperator(
        task_id = 'data_ingestor',
        python_callable=safe_main_callable
    )
    task2 = DockerOperator(
        task_id = 'run_dbt_transformation',
        image = "renzosanchez/dbt-project:latest",
        api_version = 'auto',
        auto_remove = 'never',
        docker_url = 'unix://var/run/docker.sock',
        network_mode = 'my-network',
        environment = {
            'DB_HOST': 'postgres_container',
            'DB_PORT': 5432
        },
        mount_tmp_dir=False,
        command = None,
    )
    task3 = PythonOperator(
        task_id = 'export_csv',
        python_callable= export_callable
    )

task1>>task2>>task3