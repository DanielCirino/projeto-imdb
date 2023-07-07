from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def converterArquivoParaParquet():
    pass

with DAG(
        "projeto_imdb_download_arquivos",
        description="Fazer o download diário dos arquivos do Imdb",
        schedule_interval=None,
        start_date=datetime(2023, 1, 1)
) as dag:
    task_converter_arquivos_csv_para_parquet = PythonOperator(
        task_id="tsk_baixar_arquivos_imdb",
        python_callable=converterArquivoParaParquet,
        dag=dag
    )

task_converter_arquivos_csv_para_parquet