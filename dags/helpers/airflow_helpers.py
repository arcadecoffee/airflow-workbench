def _push_values(context, values_to_push: list):
    ti = context['ti']
    for value_name in values_to_push:
        ti.xcom_push(value_name, str(getattr(ti.task, value_name, None)))


def push_values(vars: []):
    return lambda context: _push_values(context, vars)


def override_postgres_cursor(conn_id, cursor_type):
    import json
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    conn = PostgresHook.get_connection(conn_id)
    conn.extra = json.dumps({**json.loads(conn.extra), **{'cursor': cursor_type}})
    return conn
