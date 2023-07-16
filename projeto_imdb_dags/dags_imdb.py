import os
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow import DAG
from airflow.operators.python import PythonOperator

datasets = [
    {"DATASET": "name_basics",
     "URL_IMDB": "https://datasets.imdbws.com/name.basics.tsv.gz",
     "NOME_ARQUIVO_COMPACTADO": "name.basics.tsv.gz",
     "NOME_ARQUIVO_CSV": "name.basics.csv"},
    {"DATASET": "title_akas",
     "URL_IMDB": "https://datasets.imdbws.com/title.akas.tsv.gz",
     "NOME_ARQUIVO_COMPACTADO": "title.akas.tsv.gz",
     "NOME_ARQUIVO_CSV": "title.akas.csv"},
    {"DATASET": "title_basics",
     "URL_IMDB": "https://datasets.imdbws.com/title.basics.tsv.gz",
     "NOME_ARQUIVO_COMPACTADO": "title.basics.tsv.gz",
     "NOME_ARQUIVO_CSV": "title.basics.csv"},
    {"DATASET": "title_crew",
     "URL_IMDB": "https://datasets.imdbws.com/title.crew.tsv.gz",
     "NOME_ARQUIVO_COMPACTADO": "title.crew.tsv.gz",
     "NOME_ARQUIVO_CSV": "title.crew.csv"},
    {"DATASET": "title_episode",
     "URL_IMDB": "https://datasets.imdbws.com/title.episode.tsv.gz",
     "NOME_ARQUIVO_COMPACTADO": "title.episode.tsv.gz",
     "NOME_ARQUIVO_CSV": "title.episode.csv"},
    {"DATASET": "title_principals",
     "URL_IMDB": "https://datasets.imdbws.com/title.principals.tsv.gz",
     "NOME_ARQUIVO_COMPACTADO": "title.principals.tsv.gz",
     "NOME_ARQUIVO_CSV": "title.principals.csv"},
    {"DATASET": "title_ratings",
     "URL_IMDB": "https://datasets.imdbws.com/title.ratings.tsv.gz",
     "NOME_ARQUIVO_COMPACTADO": "title.ratings.tsv.gz",
     "NOME_ARQUIVO_CSV": "title.ratings.csv"}
]

for dataset in datasets:
    DATASET = dataset["DATASET"]
    URL_IMDB = dataset["URL_IMDB"]
    NOME_ARQUIVO_COMPACTADO = dataset["NOME_ARQUIVO_COMPACTADO"]
    NOME_ARQUIVO_CSV = dataset["NOME_ARQUIVO_CSV"]

    with DAG(
            f"projeto_imdb_{DATASET}_download_arquivo",
            description="Fazer o download diário dos arquivos do Imdb",
            schedule_interval=None,
            start_date=datetime(2023, 1, 1),
            # schedule="@daily",
            tags=["ingest","download"]
    ) as dag:
        task_fazer_download_arquivo = BashOperator(
            task_id=f"tsk_baixar_arquivo_imdb_{DATASET}",
            bash_command=f"python {os.getcwd()}/dags/job_download_arquivo.py -u {URL_IMDB}",
            dag=dag
        )

    with DAG(
            f"projeto_imdb_{DATASET}_descompactar_arquivo",
            description="Descompactar os arquivos baixados do IMDB",
            schedule_interval=None,
            start_date=datetime(2023, 1, 1),
            # schedule="@daily",
            tags=["transform", "unzip"]
    ) as dag:
        task_descompactar_arquivo_baixado = BashOperator(
            task_id=f"tsk_descompactar_arquivo",
            bash_command=f"python {os.getcwd()}/dags/job_descompactar_arquivo.py -f {NOME_ARQUIVO_COMPACTADO}",
            dag=dag
        )

    with DAG(
            f"projeto_imdb_{DATASET}_converter_arquivo_para_parquet",
            description="Fazer a conversão dos arquivos *.csv para .parquet",
            schedule_interval=None,
            start_date=datetime(2023, 1, 1),
            # schedule="@daily",
            tags=["transform", "convert"]
    ) as dag:
        task_converter_arquivos_csv_para_parquet = BashOperator(
            task_id="tsk_converter_arquivo_para_parquet",
            bash_command=f"python {os.getcwd()}/dags/job_converter_arquivo.py -f {NOME_ARQUIVO_CSV}",
            dag=dag
        )

    with DAG(
            f"projeto_imdb_{DATASET}_salvar_banco_dados",
            description="Fazer a inclusão dos dados do arquivo .parquet no banco de dados",
            schedule_interval=None,
            start_date=datetime(2023, 1, 1),
            # schedule="@daily",
            tags=["ingest", "banco_dados"]
    ) as dag:
        task_salvar_banco_dados = BashOperator(
            task_id="tsk_salvar_banco_dados",
            bash_command=f"python {os.getcwd()}/dags/job_inserir_banco_dados.py -f {NOME_ARQUIVO_CSV}",
            dag=dag
        )

    trigger_descompactar_arquivo_baixado = TriggerDagRunOperator(
        task_id=f"tgr_descompactar_arquivo_imdb_{DATASET}",
        trigger_dag_id=f"projeto_imdb_{DATASET}_descompactar_arquivo",
    )

    trigger_converter_arquivo_csv_para_parquet = TriggerDagRunOperator(
        task_id=f"tgr_converter_arquivo_para_parquet_{DATASET}",
        trigger_dag_id=f"projeto_imdb_{DATASET}_converter_arquivo_para_parquet",
    )

    trigger_salavar_banco_dados = TriggerDagRunOperator(
        task_id=f"tgr_salvar_banco_dados_{DATASET}",
        trigger_dag_id=f"projeto_imdb_{DATASET}_salvar_banco_dados",
    )

    task_fazer_download_arquivo >> trigger_descompactar_arquivo_baixado
    task_descompactar_arquivo_baixado >> trigger_converter_arquivo_csv_para_parquet
    task_converter_arquivos_csv_para_parquet >> trigger_salavar_banco_dados
    task_salvar_banco_dados
