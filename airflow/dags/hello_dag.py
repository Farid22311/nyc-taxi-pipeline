from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def say_hello():
    print("hello from the python task")


with DAG(
    dag_id="hello_dag",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
) as dag:
    start = BashOperator(task_id="start", bash_command="echo starting")
    hello = PythonOperator(task_id="hello", python_callable=say_hello)
    end = BashOperator(task_id="end", bash_command="echo done")

    start >> hello >> end