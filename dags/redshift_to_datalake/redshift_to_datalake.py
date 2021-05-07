import os
from pathlib import Path

import yaml

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.utils.dates import days_ago

from airflow.models.connection import Connection

from helpers.airflow_helpers import push_values, override_postgres_cursor
from helpers.datatype_converter import convert_datatype


base_dir = Path(__file__).resolve().parent
sql_dir = base_dir.joinpath('sql')
config_file = base_dir.joinpath('unload_configs.yaml')

configs = yaml.load(config_file.read_text(), Loader=yaml.BaseLoader)

get_columns_sql = sql_dir.joinpath('get_columns.sql').read_text()

unload_bucket = f'sf-dataplatform-redshift-unloads-{os.environ.get("SF_ENV", "local")}'


def get_columns(conn_id, object_name):
    schema_name, table_name = object_name.split('.')
    params = { 'schema_name': schema_name, 'table_name': table_name }

    hook = PostgresHook(connection=override_postgres_cursor(conn_id, 'namedtuplecursor'))

    columns = []
    for column_spec in hook.get_records(get_columns_sql, params):
        columns.append({
            'ordinal_position': column_spec.ordinal_position,
            'name': column_spec.column_name,
            'datatype': convert_datatype(column_spec.data_type, column_spec.character_maximum_length,
                                         column_spec.numeric_precision, column_spec.numeric_scale)
        })

    get_current_context()['ti'].xcom_push('columns', columns)


def generate_tasks(dag, src_conn_id, unload_object, unload_config):
    schema, table = unload_object.split('.')

    table_columns = PythonOperator(
        task_id=f'get_columns-{unload_object}',
        do_xcom_push=True,
        python_callable=get_columns,
        op_args=[src_conn_id, unload_object]
    )

    if unload_config['unload_type'] == 'full':
        unload_data = RedshiftToS3Operator(
            task_id=f'unload_full-{unload_object}',
            s3_bucket=unload_bucket,
            s3_key='jg/raw/{{ params.schema }}_{{ params.table }}/{{ ts_nodash }}/{{ macros.datetime.utcnow().timestamp() | int }}/',
            redshift_conn_id=src_conn_id,
            schema=schema,
            table=table,
            table_as_file_name=False,
            unload_options=['PARQUET'],
            params={'schema': schema, 'table': table},
            on_execute_callback=(lambda context: push_values(context, ['schema', 'table', 's3_bucket', 's3_key']))
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
