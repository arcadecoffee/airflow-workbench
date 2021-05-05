import os
import uuid
from pathlib import Path

from pprint import pprint

import yaml

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator

from redshift_to_datalake.helpers.datatype_converter import convert_datatype


base_dir = Path(__file__).resolve().parent
sql_dir = base_dir.joinpath('sql')
config_file = base_dir.joinpath('unload_configs.yaml')

configs = yaml.load(config_file.read_text(), Loader=yaml.BaseLoader)

get_columns_sql = sql_dir.joinpath('get_columns.sql').read_text()

unload_bucket = f'sf-dataplatform-redshift-unloads-{os.environ.get("SF_ENV", "local")}'


def get_columns(conn_id, object_name):
    params = {
        'schema_name': object_name.split('.')[0],
        'table_name': object_name.split('.')[1]
    }
    columns = []
    for ordinal_position, column_name, data_type, character_maximum_length, numeric_precision, numeric_scale in \
            PostgresHook(conn_id).get_records(get_columns_sql, params):
        columns.append([ordinal_position, column_name,
                        convert_datatype(data_type, character_maximum_length, numeric_precision, numeric_scale)])
    return columns


def forward_unload_params(context):
    ti = context['ti']
    pprint(vars(ti.task), indent=2)
    ti.xcom_push('s3_bucket', ti.task.s3_bucket)
    ti.xcom_push('s3_key', ti.task.s3_key)
    ti.xcom_push('s3_uri', f's3://{ti.task.s3_bucket}/{ti.task.s3_key}')


def generate_tasks(dag, src_conn_id, unload_object, unload_config):
    schema, table = unload_object.split('.')

    if unload_config['unload_type'] == 'full':
        table_columns = PythonOperator(
            task_id=f'get_columns-{unload_object}',
            do_xcom_push=True,
            python_callable=get_columns,
            op_args=[src_conn_id, unload_object]
        )

        unload_data = RedshiftToS3Operator(
            task_id=f'unload_data-{unload_object}',
            s3_bucket=unload_bucket,
            s3_key='jg/raw/{{ params.schema }}_{{ params.table }}/{{ ts_nodash }}/{{ macros.datetime.utcnow().timestamp() | int }}/',
            redshift_conn_id=src_conn_id,
            schema=schema,
            table=table,
            table_as_file_name=False,
            unload_options=['PARQUET'],
            params={'schema': schema, 'table': table},
            on_execute_callback=forward_unload_params
        )

        create_table = DummyOperator(task_id=f'create_table-{unload_object}')
        start_msck_repair = DummyOperator(task_id=f'start_msck_repair-{unload_object}')
        monitor_msck_repair = DummyOperator(task_id=f'monitor_msck_repair-{unload_object}')
        [table_columns, unload_data] >> create_table >> start_msck_repair >> monitor_msck_repair
        return [[table_columns, unload_data], monitor_msck_repair]


def generate_dag(dag_id, dag_config):
    with DAG(dag_id, start_date=days_ago(2), schedule_interval='@daily', catchup=True) as dag:
        globals()[dag_id] = dag
        start = DummyOperator(task_id='start')
        end = DummyOperator(task_id='end')
        for src_conn_id, src_objects in dag_config.items():
            for src_object, config in src_objects.items():
                tasks = generate_tasks(dag, src_conn_id, src_object, config)
                start >> tasks[0]
                tasks[-1] >> end


for dag_name, dag_config in configs.items():
    generate_dag(dag_name, dag_config)
