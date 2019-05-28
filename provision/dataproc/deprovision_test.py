#!/usr/bin/env python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from data_common.provision import provision_dataproc

from data_common.dictionary import dictionary as d
from data_common.config.configurer import get_conf


def print_intro():
    print('\n\nprinting intro\n\n')


conf = get_conf()

namespace = conf.default_namespace


default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(seconds=10),
    'retries': 2
}


with DAG('deprovision_test',
         default_args=default_args,
         start_date=datetime.now() - timedelta(seconds=10),
         schedule_interval=None
         # schedule_interval='0 * * * *',
         ) as dag:

    intro = PythonOperator(
        task_id='intro',
        python_callable=print_intro
    )

    terraform_deprovision = PythonOperator(
        task_id=f'terraform_deprovision_{namespace}',
        # these arguments will be sent to python callable
        op_kwargs={d.UNIQUE_NAME: namespace,
                   d.CONF: conf,
                   d.BASE_DIR: conf.namespaces_dir
                   },
        python_callable=provision_dataproc.deprovision
    )


intro >> terraform_deprovision
