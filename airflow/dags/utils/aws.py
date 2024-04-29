import boto3

# from airflow.models import Variable


def aws_sesion():
    session = boto3.Session(
        aws_access_key_id="",
        aws_secret_access_key="",
        region_name="eu-central-1"
    )
    return session

def boto3_client(aws_service):
    client = boto3.client(aws_service)

    return client
