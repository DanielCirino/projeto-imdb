import os
import zipfile

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from io import BytesIO, StringIO
from projeto_imdb.services.s3_services import s3_client
import logging
from datetime import datetime

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

        response = session.get(urlArquivo, headers=headers,verify=False)

        logging.info(f"Arquivo: {nomeArquivo} baixado com sucesso.")

        arquivoCompactado = BytesIO(response.content)
        s3_client.put_object(
            Body=arquivoCompactado, Bucket='projeto-imdb-raw',
            Key=f"{anoMesDia}/downloaded/{nomeArquivo}")

        logging.info(f"Arquivo salvo em: {anoMesDia}/{nomeArquivo}")



    except Exception as e:
        logging.error(f"Erro ao baixar e salvar o arquivo {nomeArquivo}. [{e.args}]")
        raise e

def descompactarArquivosBaixados():
    bucketRaw="projeto-imdb-raw"
    bucketStage="projeto-imdb-stage"

    objetosS3 = s3_client.list_objects(Bucket=bucketRaw)
    try:
        for obj in objetosS3.get("Contents"):
            chave = obj["Key"]
            data,diretorio,nomeArquivo =  chave.split("/")

            if diretorio == "downloaded":
                arquivoCompactado = s3_client.get_object(
                    Bucket="projeto-imdb-raw",
                    Key=chave
                ).get("Body")

            dfArquivo = pd.read_csv(arquivoCompactado, compression="gzip", header=0, sep="\t", quotechar='"', nrows=1000)
            logging.info(f"Processando arquivo {chave}")

            csv_buffer = StringIO()
            dfArquivo.to_csv(csv_buffer)

            nomeArquivoCsv = nomeArquivo.replace(".tsv.gz",".csv")
            s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucketStage,
                                 Key=nomeArquivoCsv)

            logging.info(f"Arquivo {nomeArquivoCsv} salvo no bucket stage.")

            s3_client.copy_object(
                CopySource={'Bucket': bucketRaw, 'Key': chave},
                Bucket=bucketRaw,
                Key=chave.replace("downloaded","processed")
            )
            s3_client.delete_object(Bucket=bucketRaw,Key=chave)

            logging.info(f"Arquivo {nomeArquivo} movido para a pasta de processados.")

    except Exception as e:
        logging.error(e)


if __name__ == "__main__":
    # listaArquivos = obterListaArquivosDisponiveis()
    # for nome, url in listaArquivos:
    #     fazerDownloadArquivo(url)
    #     print(nome)

    descompactarArquivosBaixados()
