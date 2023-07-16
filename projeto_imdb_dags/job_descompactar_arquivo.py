# -*- coding: utf-8 -*-

import argparse
import logging
import os
import tempfile
import setup_env

from client_s3 import clientS3
from client_spark import spark_client

parser = argparse.ArgumentParser(prog="Projeto IMDB - Descompactar",
                                 description="Job Descompactar Arquivo IMDB")

parser.add_argument("-f", "--filename")

try:
    args = parser.parse_args()
    nomeArquivoCompactado = args.filename

    bucketRaw = "projeto-imdb-raw"
    bucketStage = "projeto-imdb-stage"

    objetosS3 = clientS3.list_objects(Bucket=bucketRaw)

    for obj in objetosS3.get("Contents"):
        chave = obj["Key"]
        dataArquivo, diretorio, nomeArquivo = chave.split("/")

        if diretorio == "downloaded" and nomeArquivo == nomeArquivoCompactado:
            nomeArquivoCsv = nomeArquivo.replace(".tsv.gz", ".csv")
            diretorioArquivoCSV = nomeArquivoCsv[:-4].replace(".", "_")

            logging.info(f"Lendo arquivo arquivo {chave}")

            dfArquivo = spark_client \
                .read \
                .csv(f"s3a://{bucketRaw}/{chave}", sep="\t", header=True)

            logging.info(f"Total registros no arquivo: {dfArquivo.count()}")
            print("===========================================")
            dfArquivo.show(5)
            print("===========================================")

            logging.info(f"Salvando arquivo {nomeArquivoCsv} no bucket stage no formato *.csv...")

            dfArquivo.write.csv(
                path=f"s3a://{bucketStage}/{diretorioArquivoCSV}",
                mode="overwrite",
                header=True,
                sep=",")

            logging.info(f"Arquivo {nomeArquivoCsv} salvo no bucket stage [{bucketStage}/{diretorioArquivoCSV}].")

            # Fazer cópia do arquivo para a pasta processado e apagar da pasta de download
            novaChave = chave.replace("downloaded", "processed")

            clientS3.copy_object(
                CopySource={'Bucket': bucketRaw, 'Key': chave},
                Bucket=bucketRaw,
                Key=novaChave)

            clientS3.delete_object(Bucket=bucketRaw, Key=chave)

            logging.info(f"Arquivo {nomeArquivo} movido para a pasta de processados.")

except Exception as e:
    logging.error(f"Erro ao descompactar o arquivo {nomeArquivoCompactado}. [{e.args}]")
    raise e
