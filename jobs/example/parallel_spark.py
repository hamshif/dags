"""
This Dag runs sets of actions for all namespaces in config (per environment)
Namespace sets in parallel (subject to airflow scheduling availability)
Set composition is described in print_intro function
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from data_common.cloud.dataproc import gen_cluster_name, dataproc_spark_cmd
from data_common.config.configurer import get_conf
from data_common.provision import provision_dataproc

from data_common.dictionary import dictionary as d


def print_intro(**kwargs):

    if 'kwargs' in kwargs:
        kwargs = kwargs['kwargs']

    print(f'kwargs: {kwargs}')

    _namespace = kwargs[d.NAMESPACE]

    _intro = f'This airflow Dag performs these tasks sequentially for namespace: {_namespace}\n'
    '1. Make sure terraform provisioning repo is cloned to airflow worker namespace\n'
    '2. Provision a dedicated dataproc cluster for spark jobs\n'
    '3. Run a configured spark job\n'
    '4. Deprovision the cluster\n'

    print(intro)


default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(seconds=10),
    'retries': 3
}


with DAG('parallel_spark_example',
         default_args=default_args,
         start_date=datetime.now() - timedelta(seconds=10),
         schedule_interval=None
         # schedule_interval='0 * * * *',
         ) as dag:

    conf = get_conf()

    project_id = conf.cloud.gcp.project
    namespaces = conf.namespaces

    # This task makes sure
    get_or_create_base_repo = PythonOperator(
        task_id=f'get_or_create_local_terraform_repo',
        # these arguments will be sent to python callable
        op_kwargs={d.CONF: conf,
                   d.BASE_DIR: conf.namespaces_dir
                   },
        python_callable=provision_dataproc.base_init
    )

    for namespace, v in namespaces.items():

        print(f'namespace: {namespace}')

        cluster_name = gen_cluster_name(conf=conf, unique_name=namespace)

        print(f'cluster_name: {cluster_name}')

        conf.spark_jobs.walk_me.args = f'gs://{namespace}/walk_me/'

        walk_me_cmd = dataproc_spark_cmd(
            project=conf.cloud.gcp.project,
            cluster=cluster_name,
            region=conf.cloud.gcp.region,
            jar_tree=conf.spark_jobs.walk_me
          )

        print(f'walk_me_cmd:\n {walk_me_cmd}')

        intro = PythonOperator(
            task_id=f'intro_{namespace}',
            op_kwargs={
                d.NAMESPACE: namespace,
            },
            python_callable=print_intro
        )

        confirm_namespace = PythonOperator(
            task_id=f'confirm_namespace_{namespace}',
            # these arguments will be sent to python callable
            op_kwargs={d.UNIQUE_NAME: namespace,
                       d.CONF: conf,
                       d.BASE_DIR: conf.namespaces_dir,
                       d.NEW_STATE: True
                       },
            python_callable=provision_dataproc.namespace_init
        )

        terraform_provision = PythonOperator(
            task_id=f'terraform_provision_{namespace}',
            # these arguments will be sent to python callable
            op_kwargs={d.UNIQUE_NAME: namespace,
                       d.CONF: conf,
                       d.BASE_DIR: conf.namespaces_dir,
                       d.NEW_STATE: True
                       },
            python_callable=provision_dataproc.provision
        )

        walk_me_job = BashOperator(
            task_id=f'walk_me_spark_job_{namespace}',
            bash_command=walk_me_cmd
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

        get_or_create_base_repo >> intro >> confirm_namespace >> terraform_provision >> walk_me_job >> terraform_deprovision


