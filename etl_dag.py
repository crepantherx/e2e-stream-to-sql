from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.utils.email import send_email


@task(on_failure_callback=lambda context: send_email_notification(context, success=False))
def execute_use(fn):
    exec(open('./dags/utils/use.py').read())


@task(on_failure_callback=lambda context: send_email_notification(context, success=False))
def execute_len():
    exec(open('./dags/utils/len.py').read())


def send_email_notification(context, success=True):
    """
    Send an email notification for task success or failure.
    """
    print('Sending Logs')
    if success:
        subject = "Task Succeeded"
        body = f"The task {context['task_instance'].task_id} succeeded."
    else:
        subject = "Task Failed"
        body = f"The task {context['task_instance'].task_id} failed."

    send_email('work.crepantherx@gmail.com', subject, body)


default_args = {
    'owner': 'crepantherx',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'execute_python_script_taskflow',
    default_args=default_args,
    description='A simple DAG to execute a Python script using TaskFlow API',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

with dag:
    execute_use(execute_len())
