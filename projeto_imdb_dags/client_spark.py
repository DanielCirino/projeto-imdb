import os
import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.models import Variable
import logging

PYTHON_LOCATION = sys.executable
os.environ["PYSPARK_PYTHON"] = PYTHON_LOCATION
logging.info(f"PYTHON_LOCATION--->{PYTHON_LOCATION}")


conf = pyspark.SparkConf()
conf.setAppName('projeto-imdb')
conf.setMaster("local[*]")

conf.set("spark.hadoop.fs.s3a.endpoint", Variable.get("AWS_ENDPOINT")) \
    .set("spark.hadoop.fs.s3a.endpoint.region", Variable.get("AWS_REGION")) \
    .set("spark.hadoop.fs.s3a.access.key", Variable.get("AWS_ACCESS_KEY_ID")) \
    .set("spark.hadoop.fs.s3a.secret.key", Variable.get("AWS_SECRET_ACCESS_KEY")) \
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .set("spark.hadoop.fs.s3a.connection.ssl.enabled", False) \
    .set("spark.hadoop.com.amazonaws.services.s3.enableV2", True) \
    .set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace") \
    .set("spark.hadoop.fs.s3a.fast.upload", True) \
    .set("spark.hadoop.fs.s3a.path.style.access", True) \
    .set("spark.hadoop.fs.s3a.committer.name", "magic") \
    .set("spark.sql.sources.commitProtocolClass",
         "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
    .set("parquet.enable.summary-metadata", False) \
    .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", False)


sc = pyspark.SparkContext(conf=conf).getOrCreate()

spark_client = SparkSession(sc)
