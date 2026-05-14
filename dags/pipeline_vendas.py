from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'engenharia_dados',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_localiza_desafio',
    default_args=default_args,
    description='Pipeline de ingestao, DQ (Metadata-driven) e processamento PySpark',
    schedule_interval=None, # Execução sob demanda para fins de teste
    catchup=False,
    tags=['desafio', 'pyspark', 'great_expectations'],
) as dag:

    # O BashOperator chama o Python localmente no container. 
    # Como o PySpark já está no requirements.txt, o motor executa dentro da task,
    # minimizando a necessidade de instanciar clusters externos (redução de overhead).
    executar_pipeline = BashOperator(
        task_id='run_pyspark_and_dq',
        bash_command='python -u /opt/airflow/src/process_data.py',
    )

    executar_pipeline