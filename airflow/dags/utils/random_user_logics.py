import logging

import awswrangler as wr
import boto3
import pandas as pd
from airflow.models import Variable
from faker import Faker

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)


def aws_sesion():
    session = boto3.Session(
                    aws_access_key_id=Variable.get('access_key'),
                    aws_secret_access_key=Variable.get('secret_key'),
                    region_name="eu-central-1"
    )
    return session


def boto3_client(aws_service):

    client = boto3.client(aws_service,
                          aws_access_key_id=Variable.get('access_key'),
                          aws_secret_access_key=Variable.get('secret_key'),
                          region_name="eu-central-1")

    return client


def random_profile_data_generator(total_records: int):
    """
       Function that generates random profile for
       different individuals and build a pandas dataframe
       based on the number of profile specified.

       params:
            total_records: Total number of random profiles
            to generate, needs to be an integer, e.g 100, 2.
    """

    sample = Faker()
    logging.info("finished faker module instantiation")

    df = pd.DataFrame(
        [sample.profile() for profile in range(total_records)])
    logging.info(f"Dataframe created with {df.shape[1]}\
                              records and {df.shape[0]} columns")

# filtering out unproblematic column out for write to the lake
    df_final = df[[
        "job",
        "company",
        "ssn",
        "residence",
        "blood_group",
        "username",
        "name",
        "sex",
        "address",
        "mail"
    ]]

    return df_final


def extract_random_profile_to_s3():
    wr.s3.to_parquet(
                    df=random_profile_data_generator(10),
                    path="s3://random-user-extraction/random_user_profile/",
                    boto3_session=aws_sesion(),
                    mode="append",
                    dataset=True
                    )
    return "Data successfully written to s3"


def get_latest_s3_object():
    """
        Get latest objects in an s3 bucket in a specified path/folder
    params:
        bucket_name: amazon s3 bucket name
        bucket_prefix: the path/folder where the objects resides
                        e.g test or test/inner_test or test/inner/inner
    """
    logging.info("Checking if key specified in variable for backfilling")
    backfill_object_key = Variable.get('historical_random_object_key')
    if backfill_object_key:
        logging.info("Retrieve key specified in airflow variable for backfill")
        return backfill_object_key

    else:
        client = boto3_client("s3")
        paginator = client.get_paginator("list_objects")
        response_iterator = paginator.paginate(
            Bucket='random-user-extraction',
            Prefix='random_user_profile',
            PaginationConfig={
                'PageSize': 1000
                    }
            )
        response_iterator = [response for response in response_iterator]

        response_list = [i for i in response_iterator[0]["Contents"]]

        latest_object_raw = sorted(response_list,
                                   key=lambda d: d["LastModified"],
                                   reverse=True)

        latest_file_key = latest_object_raw[0]["Key"]

        return latest_file_key
