from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.helpers import chain, cross_downstream

from random import seed, random

default_arguments = {'owner':'Jingyi Zhou', 'start_date':days_ago(1)}

# dag = DAG('core_concepts', schedule_interval='@daily', catchup=False)

with DAG(
    'core_concepts', 
    schedule_interval='@daily', 
    catchup=False,
    default_args=default_arguments
) as dag:
    bash_task = BashOperator(
        task_id="bash_command", 
        bash_command="echo $today", 
        env={"today":"2020-09-26"}
    )

    def print_random_number(number):
        seed(number)
        print(random()) 

    python_task = PythonOperator(
        task_id="python_function", python_callable=print_random_number, op_args=[1],
    )

# define dependency
bash_task >> python_task
# bash_task.set_downstream(python_task)


# op1 >> op2 >> op3 >> op4
# chain(op1, op2, op3, op4)

# cross_downstream([op1, op2], [op3, op4])
# [op1, op2] >> [op3]
# [op1, op2] >> [op4]




