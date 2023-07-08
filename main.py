import os
import pyspark
from pyspark.sql import SparkSession
import projeto_imdb

if __name__ == '__main__':
    conf = pyspark.SparkConf()

    # Criando uma sessão com o Spark que existe localmente(atualmente configurado junto com o JupyterLab)
    conf.setMaster("local[1]")
    conf.set("spark.driver.host", "127.0.0.1") \
        .set("spark.sql.sources.commitProtocolClass",
             "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
        .set("parquet.enable.summary-metadata", "false") \
        .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .set("spark.driver.port", "7077") \
        .set("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT_URL")) \
        .set("spark.hadoop.fs.s3a.endpoint.region", os.getenv("S3_AWS_REGION_NAME")) \
        .set("spark.hadoop.fs.s3a.access.key", os.getenv("S3_AWS_ACCESS_KEY_ID")) \
        .set("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_AWS_SECRET_ACCESS_KEY")) \
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .set("spark.hadoop.com.amazonaws.services.s3.enableV2", "true") \
        .set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace") \
        .set("spark.hadoop.fs.s3a.fast.upload", True) \
        .set("spark.hadoop.fs.s3a.path.style.access", True) \
        .set("spark.hadoop.fs.s3a.committer.name", "directory") \
        .set("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/staging")

    conf.setAppName('projeto-imdb')
    sc = pyspark.SparkContext(conf=conf)

    spark = SparkSession(sc)

    rdd = spark.read.csv(f"s3a://projeto-imdb-raw/2023-07-07/downloaded/title.ratings.tsv.gz", sep="\t", header=True)

    rdd.write.option("header", True) \
        .option("delimiter", ",") \
        .csv(f"s3a://projeto-imdb-raw/title.ratings.csv")

print(rdd.show(5))
