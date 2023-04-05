from airflow.models import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
# Definindo o DAG com o context maanger

with DAG(
    'first_dag',
    start_date=days_ago(2),
    schedule_interval='@daily' # todos os dias as 00h00
) as dag:
    task1 = EmptyOperator(task_id='task1')
    task2 = EmptyOperator(task_id='task2')
    task3 = EmptyOperator(task_id='task3')

    task4 = BashOperator(task_id='cria_pasta',
                         bash_command='mkdir -p "/home/bruno/Documents/airflow_alura/pasta={{data_interval_end}}"'
                         )
    
    # task1 vai executar primeiro
    task1 >> [task2,task3] 


    # task3 executa primeiro. depois a 4 eh executada
    task3 >> task4 

