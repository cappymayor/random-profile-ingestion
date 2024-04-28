import boto3

# from airflow.models import Variable


def aws_sesion():
    session = boto3.Session(
        aws_access_key_id="AKIASWZHRVTXAIS6CI4N",
        aws_secret_access_key="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",
        region_name="eu-central-1"
    )
    return session
