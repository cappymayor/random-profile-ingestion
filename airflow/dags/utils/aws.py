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


def get_s3_object_uri(bucket_name, object_name, bucket_folder=None):
    """
       Function to retrive an s3 object URI (Unique Resource Identifier)
    """
    if bucket_folder == None:
        return f"s3://{bucket_name}/{object_name}"
    else:
        return f"s3://{bucket_name}/{bucket_folder}/{object_name}"


def get_latest_s3_object(bucket_name, bucket_prefix):
    """
        Get latest objects in an s3 bucket in a specified path/folder
    params: 
        bucket_name: amazon s3 bucket name
        bucket_prefix: the path/folder where the objects resides
                        e.g test or test/inner_test or test/inner/inner
    """

    client = boto3_client("s3")
    paginator = client.get_paginator("list_objects")
    response_iterator = paginator.paginate(
    Bucket=bucket_name,
    Prefix=bucket_prefix,
    PaginationConfig={
        'PageSize': 1000
    }
)
    response_iterator =  [response for response in response_iterator]

    response_list = [i for i in  response_iterator[0]["Contents"]]

    latest_object_raw =  sorted(response_list, key=lambda d : d["LastModified"], reverse=True)[0]

    prefix_len = len(bucket_prefix)

    latest_file = latest_object_raw["Key"][prefix_len + 1:]

    return latest_file
