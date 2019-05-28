"""
This Dag runs sets of actions for all namespaces in config (per environment)
1. Makes sure a bucket exists for the namespace
2. Uploads example csv data (used for spark jobs in the adjacent job)
"""
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from data_common.config.configurer import get_conf

from data_common.dictionary import dictionary as d
from data_common.provision.gs_buckets import s_confirm_bucket

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(seconds=10),
    'retries': 3
}

with DAG('onboard_namespaces_example',
         default_args=default_args,
         start_date=datetime.now() - timedelta(seconds=10),
         schedule_interval=None
         # schedule_interval='0 * * * *',
         ) as dag:

    conf = get_conf()

    project_id = conf.cloud.gcp.project
    namespaces = conf.namespaces

    current_dir_path = os.path.dirname(os.path.realpath(__file__))
    print(f"\ncurrent_dir_path: {current_dir_path}\n")

    sample_data_path = f'{current_dir_path}/walk_me'

    for namespace, v in namespaces.items():

        print(f'namespace: {namespace}')

        confirm_bucket = PythonOperator(
            task_id=f'confirm_bucket_{namespace}',
            # these arguments will be sent to python callable
            op_kwargs={
                d.BUCKET_NAME: namespace,
                d.PROJECT: project_id,
                d.LOCATION: 'default'
            },
            python_callable=s_confirm_bucket
        )

        # confirm_cmd = f'gsutil mb gs://{namespace}'
        #
        # confirm_bucket = BashOperator(
        #     task_id=f'confirm_bucket_{namespace}',
        #     bash_command=confirm_cmd
        # )

        upload_cmd = f"gsutil cp -r {sample_data_path} gs://{namespace}"

        print(f'upload_cmd: {upload_cmd}')

        upload = BashOperator(
            task_id=f'upload_example_data_{namespace}',
            bash_command=upload_cmd
        )

        confirm_bucket >> upload



