import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import \
    S3ToRedshiftOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from utils.random_user_logics import (extract_random_profile_to_s3,
                                      get_latest_s3_object)

DAG_ID = 'random-user-ingestion'

default_args = {
    'owner': 'data-eng',
    'start_date': datetime.datetime(2021, 11, 15),
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=5)
}


dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
    default_view="graph"
)


extract_random_profile_to_s3 = PythonOperator(
    dag=dag,
    task_id='extract_random_profile_to_s3',
    python_callable=extract_random_profile_to_s3
)

get_latest_s3_object = PythonOperator(
    dag=dag,
    task_id='get_latest_s3_object',
    python_callable=get_latest_s3_object
)

create_table = SQLExecuteQueryOperator(
    dag=dag,
    task_id='create_table',
    sql="./sql/create_table.sql",
    conn_id='redshift_default'
)

copy_object_to_redshift = S3ToRedshiftOperator(
    dag=dag,
    task_id='copy_object_to_redshift',
    schema='public',
    table='random_profile',
    s3_bucket='random-user-extraction',
    s3_key="{{ ti.xcom_pull(task_ids='get_latest_s3_object') }}",
    redshift_conn_id='redshift_default',
    aws_conn_id='aws_default',
    copy_options=['FORMAT AS PARQUET'],
    method='UPSERT',
    upsert_keys=["name", "username", "mail"],
    column_list=[
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
    ]
)

extract_random_profile_to_s3 >> get_latest_s3_object >> \
            create_table >> copy_object_to_redshift
