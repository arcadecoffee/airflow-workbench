import logging
import os
from pathlib import Path, PosixPath

import yaml

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.amazon.aws.hooks.athena import AWSAthenaHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

from custom_operators.enhanced_redshift_to_s3 import EnhancedRedshiftToS3Operator
from helpers.airflow_helpers import push_values, override_postgres_cursor
from helpers.datatype_converter import convert_datatype

base_dir = Path(__file__).resolve().parent
sql_file_path = base_dir.joinpath('sql')

config_file = base_dir.joinpath('unload_configs.yaml')

get_columns_sql_file = 'get_columns.sql'
full_unload_sql_template_file = 'unload-full.sql.jinja2'

configs = yaml.load(config_file.read_text(), Loader=yaml.BaseLoader)

env = os.environ.get("SF_ENV", "local")
iam_role = 'arn:aws:iam::850077434821:role/redshift-data-platform-s3'
unload_bucket = f'sf-dataplatform-redshift-unloads-{env}'
athena_database = f'sfdl_{env}_redshift'
key_template = '{{ params.schema }}_{{ params.table }}/{{ ds_nodash }}/{{ macros.datetime.utcnow().timestamp() | int }}/'


#######################################


def generate_dags():
    for dag_name, dag_config in configs.get('dags', {}).items():
        generate_dag(dag_name, dag_config)


#######################################


def generate_dag(dag_id, dag_config):
    schedule_interval = dag_config.get('schedule_interval')
    with DAG(dag_id, start_date=days_ago(2), schedule_interval=schedule_interval, catchup=True,
             template_searchpath=str(sql_file_path)) as dag:
        globals()[dag_id] = dag
        start = DummyOperator(task_id='start')
        end = DummyOperator(task_id='end')
        for object_name, object_config in dag_config.items():
            tasks = generate_tasks(dag, object_name, object_config)
            start >> tasks[0]
            tasks[-1] >> end


#######################################


def generate_tasks(dag, unload_object, unload_config):
    schema, table = unload_object.split('.')
    connection_id = unload_config.get('connection_id')

    table_columns = PythonOperator(
        task_id=f'get_columns-{unload_object}',
        do_xcom_push=True,
        provide_context=True,
        python_callable=get_columns,
        op_args=[connection_id, unload_object]
    )

    if unload_config['unload_type'] == 'full':
        unload_data = EnhancedRedshiftToS3Operator(
            task_id=f'unload_full-{unload_object}',
            s3_bucket=unload_bucket,
            s3_key=f'raw/{key_template}',
            redshift_conn_id=connection_id,
            iam_role=iam_role,
            select_query=full_unload_sql_template_file,
            table_as_file_name=False,
            unload_options=['PARQUET'],
            params={'schema': schema, 'table': table,
                    'column_src_task': table_columns.task_id, 'column_src_key': 'columns'},
            on_execute_callback=push_values(['params', 's3_bucket', 's3_key'])
        )

        create_table = PythonOperator(
            task_id=f'create_table-{unload_object}',
            do_xcom_push=True,
            provide_context=True,
            python_callable=create_athena_table,
            templates_dict={'drop_query': 'drop-table.sql.jinja2', 'create_query': 'create-full.sql.jinja2'},
            templates_exts=['.sql.jinja2'],
            params={'database': athena_database, 'schema': schema, 'table': table,
                    'column_src_task': table_columns.task_id, 'column_src_key': 'columns',
                    's3_src_task': unload_data.task_id, 's3_src_bucket': 's3_bucket', 's3_src_key': 's3_key'},
            op_args=[unload_object]
        )

    else:
        unload_data = DummyOperator(task_id=f'unload_other-{unload_object}')
        create_table = DummyOperator(task_id=f'create_table-{unload_object}')

    start_msck_repair = DummyOperator(task_id=f'start_msck_repair-{unload_object}')
    monitor_msck_repair = DummyOperator(task_id=f'monitor_msck_repair-{unload_object}')
    table_columns >> unload_data >> create_table >> start_msck_repair >> monitor_msck_repair
    return [table_columns, monitor_msck_repair]


#######################################


def get_columns(conn_id, object_name, **context):
    schema_name, table_name = object_name.split('.')
    params = {'schema_name': schema_name, 'table_name': table_name}
    hook = PostgresHook(connection=override_postgres_cursor(conn_id, 'realdictcursor'))
    sql_statement = sql_file_path.joinpath(get_columns_sql_file).read_text()

    columns = []
    logging.info(f'Running query:\n{sql_statement}\nParameters:{params}')
    for column_spec in hook.get_records(sql_statement, params):
        column_spec['athena_datatype'] = convert_datatype(column_spec['data_type'],
                                                          column_spec['character_maximum_length'],
                                                          column_spec['numeric_precision'],
                                                          column_spec['numeric_scale'])
        columns.append(column_spec)
    context['ti'].xcom_push('columns', columns)


#######################################


def create_athena_table(object_name, **context):
    query_id = AWSAthenaHook().run_query(context['templates_dict']['drop_query'])
    logging.info("Drop Query: " + context['templates_dict']['drop_query'])
    logging.info("Create Query: " + context['templates_dict']['create_query'])
    return query_id


#######################################


generate_dags()
