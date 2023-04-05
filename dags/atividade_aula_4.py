from airflow.models import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator




with DAG(
        dag_id='atividade_aula_4',
        start_date=days_ago(1),
        schedule_interval='@daily'
) as dag:
    
    def cumprimentos():
        print("Boas-vindas ao Airflow!")

    task1 = EmptyOperator(task_id='task1')
    task2 = PythonOperator(task_id='cumprimentos',
                           python_callable=cumprimentos)
task1 >> task2