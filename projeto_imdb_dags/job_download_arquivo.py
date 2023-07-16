import argparse
import logging
import os
from datetime import datetime

import requests

from client_s3 import clientS3

parser = argparse.ArgumentParser(prog="Projeto IMDB - Download",
                                 description="Job Download Arquivo IMDB")

parser.add_argument("-u", "--url")

try:
    args = parser.parse_args()
    urlArquivo = args.url

    anoMesDia = datetime.now().strftime("%Y-%m-%d")
    nomeArquivo = os.path.basename(urlArquivo)

    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; "
                      "Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"}

    response = session.get(urlArquivo, headers=headers, verify=False)

    logging.info(f"Arquivo: {nomeArquivo} baixado com sucesso.")

    arquivoCompactado = response.content

    clientS3.put_object(
        Body=arquivoCompactado, Bucket='projeto-imdb-raw',
        Key=f"{anoMesDia}/downloaded/{nomeArquivo}")

    logging.info(f"Arquivo salvo em: {anoMesDia}/downloaded/{nomeArquivo}")



except Exception as e:
    logging.error(f"Erro ao baixar e salvar o arquivo {urlArquivo}. [{e.args}]")
    raise e
