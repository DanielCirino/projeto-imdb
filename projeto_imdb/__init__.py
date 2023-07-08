import os
import sys
from dotenv import load_dotenv
import logging

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON_LOCATION=sys.executable
os.environ["PYSPARK_PYTHON"] = PYTHON_LOCATION
os.environ["SPARK_HOME"]="C:/Users/Daniel/Documents/Awari/EngenhariaDados/projeto-imdb/venv/Lib/site-packages/pyspark/"

AMBIENTE = os.getenv("PROJETO_IMDB_ENV")
PATH_ARQUIVO_CONFIG = f"{ROOT_DIR}/.env.test"

logging.basicConfig(format="[%(levelname)s] [%(asctime)s] %(message)s",
                    level=logging.INFO,
                    datefmt="%d/%m/%y %H:%M:%S",
                    encoding="utf-8")

if AMBIENTE is None:
    logging.info("Variável de ambiente PROJETO_IMDB_ENV não existe.")
    AMBIENTE = 'DEV'

if AMBIENTE == 'PROD':
    PATH_ARQUIVO_CONFIG = f'{ROOT_DIR}/.env'

load_dotenv(PATH_ARQUIVO_CONFIG)

logging.info(f"Ambiente da aplicação:{AMBIENTE}")
logging.info(f"Python: {os.getenv('PYSPARK_PYTHON')}")
logging.info(f"PySpark Python: {os.path.dirname(PYTHON_LOCATION)}")