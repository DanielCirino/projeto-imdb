# Projeto Prático Engenharia de Dados
A proposta é criar um projeto de Big Data com ingestão e visualização de dados ponto a ponta.

## Problema de negócio
* É necessário disponibilizar para os cientistas de dados os dados de filmes disponibilizados na plataforma IMDB (https://datasets.imdbws.com/).
* O datalake precisa ser atualizado diariamente (a mesma frequência que os dados são atualizados pelo IMDB)
* É necessário manter um histórico dos dados originais obtidos, para que o passado possa ser validado.
* As informações de títulos lançados no Brasil precisam ser salvos no banco de dados PostgreSQL para que possam ser visualizados em uma ferramenta de BI.

## Tecnologias utilizadas
* Python
* Airflow
* MinIO
* Spark
* PostgreSQL
* Jupyter Lab
* Docker
* Git

## Docker Compose
O arquivo docker-compose responsável por "levantar" todos os serviços necessários para o projeto está disponível em <code>projeto_imdb_infra/docker-compose.yml</code>
e ele contém os seguintes serviços:
* 4 servidores de storage MinIO
* 1 servidor web para interface MinIO com Ngix
* 1 servidor de banco de dados PosgreSQL para os dados do Airflow
* 1 servidor para o Airflow Scheduler
* 1 servidor web para interface do Airflow
* 1 servidor para as rotinas de inicialização do Airflow
* 1 servidor para o Jupyter Lab
* 1 servidor de banco de dados postgreSQL para o Datawarehouse
* 1 servidor para o Metabase
* 1 servidor para o Spark com função de MASTER
* 1 servidor para o Spark com função de HISTORY
* 4 servidor para o Spark com função de WORKER 

# Fluxo de trabalho
*  Fazer diariamente o download dos arquivos disponíveis na plataforma do IMDB
    * name.basics.tsv.gz
    * title.akas.tsv.gz
    * title.basics.tsv.gz
    * title.crew.tsv.gz
    * title.episode.tsv.gz
    * title.principals.tsv.gz
    * title.ratings.tsv.gz
   
* Salvar os arquivos baixados no bucket <code>projeto-imdb-raw</code> do MinIO 
no diretório <code>/year={yyyy}/month={mm}/day={dd}/downloaded</code> a data do download do arquivo.
* Processar os arquivos baixados, e salvar no bucket <code>projeto-imdb-stage</code> no formato *.csv. 
Após o processamento mover o arquivo original para o bucket <code>projeto-imdb-raw</code> no diretório <code>/year={yyyy}/month={mm}/day={dd}/processed</code>
* Processar os arquivos do bucket <code>projeto-imdb-stage</code> e salvar os dados dos arquivos *.csv no formato *.parquet no bucket <code>projeto-imdb-analytics</code>
* Filtrar os dados referentes aos títulos que foram lançados no Brasil e salvar estes dados no banco de dados PostgreSQL na base do Datawarehouse.