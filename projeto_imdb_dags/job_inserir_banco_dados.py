# -*- coding: utf-8 -*-

import argparse
import logging

import setup_env
import client_spark


def salvarDataFrameBancoDados(df, tabela: str):
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres-server:5432/projeto_imdb") \
        .option("driver", "org.postgresql.Driver").option("dbtable", tabela) \
        .option("user", "postgres").option("password", "postgres").save(mode="overwrite")

    logging.info(f"{df.count()} registros salvos na tabela {tabela}]")


parser = argparse.ArgumentParser(prog="Projeto IMDB - Carregar",
                                 description="Job inserir dados IMDB no banco de dados")

parser.add_argument("-f", "--filename")

try:
    args = parser.parse_args()
    nomeArquivo = args.filename

    BUCKET_ANALYTICS = "projeto-imdb-analytics"
    diretorioArquivo = nomeArquivo[:-4].replace(".", "_")

    logging.info(f"Lendo arquivo arquivo *.parquet do diretório {diretorioArquivo}")

    spark_client = client_spark.obterSparkClient(f"ingest-{diretorioArquivo}")
    dfArquivo = spark_client \
        .read \
        .parquet(f"s3a://{BUCKET_ANALYTICS}/{diretorioArquivo}", header=True)

    dfArquivo.createOrReplaceTempView(diretorioArquivo)

    logging.info(f"Total registros no arquivo: {dfArquivo.count()}")

    print("===========================================")
    dfArquivo.show(5)
    print("===========================================")

    dfAkas = spark_client \
        .read \
        .parquet(f"s3a://{BUCKET_ANALYTICS}/title_akas", header=True)

    dfAkas.createOrReplaceTempView("title_akas")

    sqlTitulosBR = """
        select distinct titleId
        from title_akas 
        where 1=1
        and region='BR' 
    """

    dfTitulosBR = spark_client.sql(sqlTitulosBR)
    dfTitulosBR.createOrReplaceTempView("title_akas_br")

    print(f"Existem {dfTitulosBR.count()} titulos que foram lançados no Brasil")

    logging.info(f"Salvando dados [{diretorioArquivo}] no banco de dados")

    if diretorioArquivo == "title_akas":
        salvarDataFrameBancoDados(dfTitulosBR, f"{diretorioArquivo}_br")
        exit(0)

    if diretorioArquivo == "name_basics":
        exit(0)

    sql = f"""
        select tb.* 
        from {diretorioArquivo} tb 
            inner join title_akas_br tbr 
            on tb.tconst=tbr.titleId
    """
    dfDadosBR = spark_client.sql(sql)
    salvarDataFrameBancoDados(dfDadosBR, f"{diretorioArquivo}_br")

except Exception as e:
    logging.error(f"Erro ao converter arquivo para parquet {nomeArquivo}. [{e.args}]")
    raise e
