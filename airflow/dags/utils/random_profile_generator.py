import logging
from dateutil.tz import tzutc
from aws import boto3_client
import awswrangler as wr
import pandas as pd
from faker import Faker
from aws import aws_sesion
import json

# setting logging to enable us to debug when code fails
logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)


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
    logging.info(f"Dataframe created with {df.shape[1]} records and {df.shape[0]} columns")

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


def extract_profile_to_s3():
    wr.s3.to_parquet(
                    df=random_profile_data_generator(10),
                    path="s3://random-user-extraction/random_user_profile/",
                    boto3_session=aws_sesion(),
                    mode="append",
                    dataset=True
                    )
    return "Data successfully written to s3"

print(extract_profile_to_s3())


def get_latest_s3_object():
    
    client = boto3_client("s3")
    paginator = client.get_paginator("list_objects")
    response_iterator = paginator.paginate(
    Bucket='random-user-extraction',
    Prefix='random_user_profile',
    PaginationConfig={
        'PageSize': 1000
    }
)
    response_iterator =  [response for response in response_iterator]

    response_list = [i for i in  response_iterator[0]["Contents"]]

    latest_object_raw =  sorted(response_list, key=lambda d : d["LastModified"], reverse=True)[0]

    latest_file = latest_object_raw["Key"]

    return latest_file
