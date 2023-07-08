import logging
from datetime import datetime
import os
import requests
from s3_services import s3_client
from io import StringIO
from spark_services import spark_client

URLS_IMDB = {
   "name_basics": "https://datasets.imdbws.com/name.basics.tsv.gz",
   "title_akas": "https://datasets.imdbws.com/title.akas.tsv.gz",
   "title_basics": "https://datasets.imdbws.com/title.basics.tsv.gz",
   "title_crew": "https://datasets.imdbws.com/title.crew.tsv.gz",
   "title_episode": "https://datasets.imdbws.com/title.episode.tsv.gz",
   "title_principals": "https://datasets.imdbws.com/title.principals.tsv.gz",
   "title_ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz"
}

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

                # dfArquivo = pd.read_csv(arquivoCompactado,
                #                         compression="gzip",
                #                         header=0,
                #                         sep="\t",
                #                         quotechar='"',
                #                         nrows=200000)

                logging.info(f"Processando arquivo {chave}")
                dfArquivo = spark_client.read.csv(arquivoCompactado, sep="\t", header=True)

                csv_buffer = StringIO()
                # dfArquivo.to_csv(csv_buffer)
                dfArquivo.write.option("header", True) \
                    .option("delimiter", ",") \
                    .csv(csv_buffer)

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