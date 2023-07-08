from datetime import datetime

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from operators import fazerDownloadArquivo, descompactarArquivoBaixado, converterArquivoParaParquet
from airflow import DAG
from airflow.operators.python import PythonOperator

DATASET = "title_crew"
URL_IMDB = "https://datasets.imdbws.com/title.crew.tsv.gz"
NOME_ARQUIVO_COMPACTADO = "title.crew.tsv.gz"
NOME_ARQUIVO_CSV = "title.crew.csv"

with DAG(
        f"projeto_imdb_{DATASET}_download_arquivo",
        description="Fazer o download diário dos arquivos do Imdb",
        schedule_interval=None,
        start_date=datetime(2023, 1, 1)
) as dag:
    task_fazer_download_arquivo = PythonOperator(
        task_id=f"tsk_baixar_arquivo_imdb_{DATASET}",
        python_callable=fazerDownloadArquivo,
        op_kwargs={"urlArquivo": URL_IMDB},
        dag=dag
    )

with DAG(
        f"projeto_imdb_{DATASET}_descompactar_arquivo",
        description="Descompactar os arquivos baixados do IMDB",
        schedule_interval=None,
        start_date=datetime(2023, 1, 1)
) as dag:
    task_descompactar_arquivo_baixado = PythonOperator(
        task_id=f"tsk_descompactar_arquivo",
        python_callable=descompactarArquivoBaixado,
        op_kwargs={"nomeArquivoCompactado": NOME_ARQUIVO_COMPACTADO},
        dag=dag
    )


with DAG(
        f"projeto_imdb_{DATASET}_converter_arquivo_para_parquet",
        description="Fazer a conversão dos arquivos *.csv para .parquet",
        schedule_interval=None,
        start_date=datetime(2023, 1, 1)
) as dag:
    task_converter_arquivos_csv_para_parquet = PythonOperator(
        task_id="tsk_converter_arquivo_para_parquet",
        python_callable=converterArquivoParaParquet,
        op_kwargs={"nomeArquivo":NOME_ARQUIVO_CSV},
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

task_fazer_download_arquivo >> trigger_descompactar_arquivo_baixado
task_descompactar_arquivo_baixado >> trigger_converter_arquivo_csv_para_parquet
task_converter_arquivos_csv_para_parquet