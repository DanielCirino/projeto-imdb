import boto3
from airflow.models import Variable

s3_client = boto3.client('s3',
                         endpoint_url=Variable.get("AWS_ENDPOINT"),
                         aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
                         aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
                         aws_session_token=None,
                         config=boto3.session.Config(signature_version='s3v4'),
                         verify=False,
                         region_name=Variable.get("AWS_REGION")
                         )
