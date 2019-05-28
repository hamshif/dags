#!/usr/bin/env python

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from data_common.provision import provision_dataproc

from data_common.dictionary import dictionary as d
from data_common.config.configurer import get_conf

conf = get_conf()
namespace = conf.default_namespace


def print_intro(**kwargs):
    print('\n\nprinting intro conf\n\n')
    print(f"kwargs: {kwargs}")
    print(namespace)
    print(conf)


default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(seconds=10),
    'retries': 0
}


with DAG('provision_test',
         default_args=default_args,
         start_date=datetime.now() - timedelta(seconds=10),
         schedule_interval=None
         # schedule_interval='0 * * * *',
         ) as dag:

    intro = PythonOperator(
        task_id='intro',
        provide_context=True,
        python_callable=print_intro
    )

    terraform_provision = PythonOperator(
        task_id=f'terraform_provision',
        # these arguments will be sent to python callable
        provide_context=True,
        op_kwargs={d.UNIQUE_NAME: namespace,
                   d.CONF: conf,
                   d.BASE_DIR: conf.namespaces_dir,
                   d.NEW_STATE: True
                   },
        python_callable=provision_dataproc.provision
    )


intro >> terraform_provision
