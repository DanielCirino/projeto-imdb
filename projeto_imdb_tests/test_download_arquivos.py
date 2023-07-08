from projeto_imdb.airflow_dags import operators as operator

def test_fazer_download_arquivos_imdb():
   operator.fazerDownloadArquivos()