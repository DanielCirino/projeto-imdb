# -*- coding: utf-8 -*-

import argparse
import logging

import setup_env

from client_s3 import clientS3
import client_spark



parser = argparse.ArgumentParser(prog="Projeto IMDB - Converter",
                                 description="Job converter arquivo IMDB de *.csv para *.parquet")

parser.add_argument("-f", "--filename")

try:
    args = parser.parse_args()
    nomeArquivo = args.filename


    bucketStage = "projeto-imdb-stage"
    bucketAnalytics = "projeto-imdb-analytics"
    diretorioArquivo = nomeArquivo[:-4].replace(".", "_")

    logging.info(f"Lendo arquivos *.csv do diretório {diretorioArquivo}")

    spark_client = client_spark.obterSparkClient(f"convert-{diretorioArquivo}")

    dfArquivo = spark_client \
        .read \
        .csv(f"s3a://{bucketStage}/{diretorioArquivo}", header=True)

    logging.info(f"Total registros no arquivo: {dfArquivo.count()}")
    print("===========================================")
    dfArquivo.show(5)
    print("===========================================")

    logging.info(f"Convertendo arquivo {nomeArquivo} para parquet")

    nomeArquivoParquet = nomeArquivo.replace(".csv", ".parquet")

    dfArquivo.write.parquet(path=f"s3a://{bucketAnalytics}/{diretorioArquivo}", mode="overwrite")

    logging.info(f"Arquivo {nomeArquivoParquet} salvo no bucket analytics. [{bucketAnalytics}/{diretorioArquivo}]")

except Exception as e:
    logging.error(f"Erro ao converter arquivo para parquet {nomeArquivo}. [{e.args}]")
    raise e
