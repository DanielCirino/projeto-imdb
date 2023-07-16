# -*- coding: utf-8 -*-

import argparse
import logging

import setup_env

from client_s3 import clientS3
from client_spark import spark_client


parser = argparse.ArgumentParser(prog="Projeto IMDB - Carregar",
                                 description="Job inserir dados IMDB no banco de dados")

parser.add_argument("-f", "--filename")

try:
    args = parser.parse_args()
    nomeArquivo = args.filename

    bucketAnalytics = "projeto-imdb-stage"
    diretorioArquivo = nomeArquivo[:-4].replace(".", "_")

    logging.info(f"Lendo arquivo arquivo *.csv do diretório {diretorioArquivo}")

    dfArquivo = spark_client \
        .read \
        .csv(f"s3a://{bucketAnalytics}/{diretorioArquivo}", header=True)

    logging.info(f"Total registros no arquivo: {dfArquivo.count()}")

    print("===========================================")
    dfArquivo.show(5)
    print("===========================================")

    logging.info(f"Salvando dados no banco de dados")


    dfArquivo.write.format("jdbc")\
    .option("url", "jdbc:postgresql://postgres-server:5432/projeto_imdb") \
    .option("driver", "org.postgresql.Driver").option("dbtable", diretorioArquivo) \
    .option("user", "postgres").option("password", "postgres").save(mode="overwrite")

    logging.info(f"Dados do arquivo {bucketAnalytics}/{diretorioArquivo} salvos na tabela {diretorioArquivo}]")



except Exception as e:
    logging.error(f"Erro ao converter arquivo para parquet {nomeArquivo}. [{e.args}]")
    raise e
