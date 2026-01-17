from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


default_args = {
    'owner': 'Khalid',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


with DAG(dag_id='test_dag',
         description='experiemental dag',
         default_args=default_args,
         start_date=days_ago(1),
         schedule_interval='@daily') as dag:
    
    taskA=BashOperator(task_id='taskA',
                       bash_command="echo 'Hello world task A'")
    
    taskB=BashOperator(task_id='taskB',
                       bash_command="echo 'Hello world task B'")
    
    taskC=BashOperator(task_id='taskC',
                       bash_command="echo 'Hello world task C'")
    
    taskD=BashOperator(task_id='taskD',
                       bash_command="echo 'Hello world task D'")
    
    taskE=BashOperator(task_id='taskE',
                       bash_command="echo 'Hello world task E'")
    
    taskF=BashOperator(task_id='taskF',
                       bash_command="echo 'Hello world task F'")

taskA >> [taskB,taskC]
taskD >> [taskE,taskF]
[taskB,taskC] >> taskD