import os
import boto3

s3_client = boto3.client('s3',
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("S3_AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("S3_AWS_SECRET_ACCESS_KEY"),
    config=boto3.session.Config(signature_version='s3v4'),
    verify=False,
    region_name=os.getenv("S3_AWS_REGION_NAME")
)