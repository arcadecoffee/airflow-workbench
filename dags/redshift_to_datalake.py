from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


def generate_tasks(dag, in_ip):
    get_ip = BashOperator(task_id='get_ip', bash_command=f'echo {in_ip}')

    @dag.task(multiple_outputs=True)
    def prepare_email(input: str):
        external_ip = input
        return {
            'subject': f'Server connected from {external_ip}',
            'body': f'Seems like today your server executing Airflow is connected from the external IP {external_ip}<br>',
            'command': f'echo {external_ip}'
        }

    email_info = prepare_email(get_ip.output)
    send_email = BashOperator(
        task_id='send_email',
        bash_command=f'echo {email_info["subject"]}'
    )
    email_info >> send_email
    return [get_ip, email_info, send_email]


def generate_dag(dag_id):
    with DAG(dag_id, start_date=days_ago(2), schedule_interval='@daily', catchup=True) as dag:
        globals()[dag_id] = dag

        @dag.task()
        def start():
            print('Started')

        @dag.task()
        def end():
            print('Ended')

        tasks = generate_tasks(dag, '8.8.8.8')
        start() >> tasks[0]
        tasks[-1] >> end()


generate_dag('send_server_ip')
