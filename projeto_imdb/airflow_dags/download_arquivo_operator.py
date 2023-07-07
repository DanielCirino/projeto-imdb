import os
from io import StringIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
from bs4 import BeautifulSoup as bs
import requests
from s3_services import s3_client
import pandas as pd


def obterListaArquivosDisponiveis():
    try:
        urlDatasets = "https://datasets.imdbws.com/"
        response = requests.get(urlDatasets)
        html = bs(response.content, "html.parser")
        linksHtml = html.select("ul>a")
        listaLinks = [(link.text, link.attrs["href"]) for link in linksHtml]

        return listaLinks
    except Exception as e:
        logging.error(e)


def fazerDownloadArquivo(urlArquivo: str):
    try:
        anoMesDia = datetime.now().strftime("%Y-%m-%d")
        nomeArquivo = os.path.basename(urlArquivo)

        session = requests.Session()
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; "
                          "Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"}

        response = session.get(urlArquivo, headers=headers, verify=False)

        logging.info(f"Arquivo: {nomeArquivo} baixado com sucesso.")

        arquivoCompactado = response.content
        s3_client.put_object(
            Body=arquivoCompactado, Bucket='projeto-imdb-raw',
            Key=f"{anoMesDia}/downloaded/{nomeArquivo}")

        logging.info(f"Arquivo salvo em: {anoMesDia}/downloaded/{nomeArquivo}")



    except Exception as e:
        logging.error(f"Erro ao baixar e salvar o arquivo {nomeArquivo}. [{e.args}]")
        raise e


def fazerDownloadArquivos():
    listaArquivos = obterListaArquivosDisponiveis()

    for nome, url in listaArquivos:
        fazerDownloadArquivo(url)


def descompactarArquivosBaixados():
    bucketRaw = "projeto-imdb-raw"
    bucketStage = "projeto-imdb-stage"

    objetosS3 = s3_client.list_objects(Bucket=bucketRaw)
    try:
        for obj in objetosS3.get("Contents"):
            chave = obj["Key"]
            data, diretorio, nomeArquivo = chave.split("/")

            if diretorio == "downloaded":
                arquivoCompactado = s3_client.get_object(
                    Bucket="projeto-imdb-raw",
                    Key=chave
                ).get("Body")

            dfArquivo = pd.read_csv(arquivoCompactado, compression="gzip", header=0, sep="\t", quotechar='"')
            logging.info(f"Processando arquivo {chave}")

            csv_buffer = StringIO()
            dfArquivo.to_csv(csv_buffer)

            nomeArquivoCsv = nomeArquivo.replace(".tsv.gz", ".csv")
            s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucketStage,
                                 Key=nomeArquivoCsv)

            logging.info(f"Arquivo {nomeArquivoCsv} salvo no bucket stage.")

            s3_client.copy_object(
                CopySource={'Bucket': bucketRaw, 'Key': chave},
                Bucket=bucketRaw,
                Key=chave.replace("downloaded", "processed")
            )
            s3_client.delete_object(Bucket=bucketRaw, Key=chave)

            logging.info(f"Arquivo {nomeArquivo} movido para a pasta de processados.")

    except Exception as e:
        logging.error(e)


with DAG(
        "projeto_imdb_download_arquivos",
        description="Fazer o download diário dos arquivos do Imdb",
        schedule_interval=None,
        start_date=datetime(2023, 1, 1)
) as dag:
    task_fazer_download_arquivos = PythonOperator(
        task_id="tsk_baixar_arquivos_imdb",
        python_callable=fazerDownloadArquivos,
        dag=dag
    )

    task_descompactar_arquivos_baixados = PythonOperator(
        task_id="tsk_descompactar_arquivos_imdb",
        python_callable=descompactarArquivosBaixados,
        dag=dag
    )

task_fazer_download_arquivos >> task_descompactar_arquivos_baixados
