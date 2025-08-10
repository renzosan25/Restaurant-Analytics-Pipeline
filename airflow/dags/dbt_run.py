import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator


default_args= {
    'description' : 'A DAG to run dbt and test models',
    'start_date' : datetime(2025,8,4),
    'catchup' : False,
    'retries' : 2,
    'retry_delay' : timedelta(minutes=3)
}


dag = DAG(
    dag_id='map-dbt-run',
    default_args=default_args,
    schedule=None
)


with dag:
    task1 = DockerOperator(
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
