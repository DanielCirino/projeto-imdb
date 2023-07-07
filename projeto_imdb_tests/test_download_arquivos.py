from projeto_imdb.airflow_dags import download_arquivo_operator as operator

def test_fazer_download_arquivos_imdb():
   operator.fazerDownloadArquivos()