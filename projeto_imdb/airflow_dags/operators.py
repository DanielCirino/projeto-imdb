import logging
import os
from datetime import datetime
from io import StringIO

import requests
from bs4 import BeautifulSoup as bs

from s3_services import s3_client
from spark_services import spark_client
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


def descompactarArquivoBaixado(nomeArquivoCompactado: str):
    bucketRaw = "projeto-imdb-raw"
    bucketStage = "projeto-imdb-stage"

    objetosS3 = s3_client.list_objects(Bucket=bucketRaw)

    for obj in objetosS3.get("Contents"):
        chave = obj["Key"]
        dataArquivo, diretorio, nomeArquivo = chave.split("/")

        if diretorio == "downloaded" and nomeArquivo == nomeArquivoCompactado:
            arquivoCompactado = s3_client.get_object(
                Bucket="projeto-imdb-raw",
                Key=chave
            ).get("Body")

            logging.info(f"Descompactando arquivo {chave}")
            nomeArquivoCsv = nomeArquivo.replace(".tsv.gz", ".csv")

            # rdd = spark_client.read.csv(f"s3a://{bucketRaw}/{chave}", sep="\t", header=True)
            #
            # rdd.write.option("header", True) \
            #     .option("delimiter", ",") \
            #     .csv(f"s3a://{bucketStage}/{nomeArquivoCsv}")

            dfArquivo = pd.read_csv(arquivoCompactado, compression="gzip", header=0, sep="\t", quotechar='"')
            logging.info(f"Processando arquivo {chave}")

            csv_buffer = StringIO()
            dfArquivo.to_csv(csv_buffer,index=False)

            s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucketStage,
                                 Key=nomeArquivoCsv)


            logging.info(f"Arquivo {nomeArquivoCsv} salvo no bucket stage.")

            # Fazer cópia do arquivo para a pasta processado e apagar da pasta de download
            s3_client.copy_object(
                CopySource={'Bucket': bucketRaw, 'Key': chave},
                Bucket=bucketRaw,
                Key=chave.replace("downloaded", "processed")
            )
            s3_client.delete_object(Bucket=bucketRaw, Key=chave)

            logging.info(f"Arquivo {nomeArquivo} movido para a pasta de processados.")


def converterArquivoParaParquet():
    pass
