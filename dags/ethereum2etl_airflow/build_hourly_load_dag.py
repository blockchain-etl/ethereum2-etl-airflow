from __future__ import print_function

import logging
import os
from datetime import datetime, timedelta

from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor
from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning

from ethereum2etl_airflow.bigquery_utils import submit_bigquery_job, create_dataset, read_bigquery_schema_from_file
from ethereum2etl_airflow.file_utils import read_file

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_hourly_load_dag(
        dag_id,
        output_bucket,
        destination_dataset_project_id,
        chain='ethereum2',
        notification_emails=None,
        load_start_date=datetime(2018, 8, 4),
        load_end_date=None,
        load_schedule_interval='0 0 * * *'
):
    """Build Load DAG"""

    dataset_name = f'crypto_{chain}'

    if not destination_dataset_project_id:
        raise ValueError('destination_dataset_project_id is required')

    default_dag_args = {
        'depends_on_past': False,
        'start_date': load_start_date,
        'end_date': load_end_date,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    environment = {
        'dataset_name': dataset_name,
        'destination_dataset_project_id': destination_dataset_project_id,
    }

    # Define a DAG (directed acyclic graph) of tasks.
    dag = models.DAG(
        dag_id,
        catchup=False if load_end_date is None else True,
        schedule_interval=load_schedule_interval,
        default_args=default_dag_args)

    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')

    def add_load_tasks(task, time_partitioning_field='block_timestamp', wait_load_task=None, file_name=None):
        if file_name is None:
            file_name = task
        if wait_load_task is not None:
            wait_sensor = ExternalTaskSensor(
                task_id='wait_latest_{task}'.format(task=task),
                external_dag_id='eth2_mainnet_hourly_export_dag',
                external_task_id=wait_load_task,
                execution_delta=timedelta(minutes=0),
                priority_weight=0,
                mode='reschedule',
                poke_interval=5 * 60,
                timeout=60 * 60 * 2,
                dag=dag)
        else:
            wait_sensor = GoogleCloudStorageObjectSensor(
                task_id='wait_latest_{task}'.format(task=task),
                timeout=60 * 60,
                poke_interval=60,
                bucket=output_bucket,
                object='export_hourly/{task}/block_date={datestamp}/{{{{execution_date.strftime("%H")}}}}/{file_name}.json'
                    .format(task=task, datestamp='{{ds}}', file_name=file_name),
                dag=dag
            )

        def load_task(ds, **kwargs):
            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig()
            schema_path = os.path.join(dags_folder, 'ethereum2etl_resources/stages/load/schemas/{file_name}.json'.format(file_name=file_name))
            job_config.schema = read_bigquery_schema_from_file(schema_path)
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.write_disposition = 'WRITE_TRUNCATE'
            job_config.ignore_unknown_values = True
            if time_partitioning_field is not None:
                job_config.time_partitioning = TimePartitioning(field=time_partitioning_field)

            export_location_uri = 'gs://{bucket}/export_hourly'.format(bucket=output_bucket)
            uri = '{export_location_uri}/{task}/*.json'.format(export_location_uri=export_location_uri, task=task)
            table_ref = create_dataset(client, dataset_name, destination_dataset_project_id).table(task)
            load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
            submit_bigquery_job(load_job, job_config)
            assert load_job.state == 'DONE'

        load_operator = PythonOperator(
            task_id='load_{task}'.format(task=task),
            python_callable=load_task,
            provide_context=True,
            execution_timeout=timedelta(minutes=30),
            dag=dag
        )

        wait_sensor >> load_operator
        return load_operator

    def add_verify_tasks(task, dependencies=None):
        # The queries in verify/sqls will fail when the condition is not met
        # Have to use this trick since the Python 2 version of BigQueryCheckOperator doesn't support standard SQL
        # and legacy SQL can't be used to query partitioned tables.
        sql_path = os.path.join(dags_folder, 'ethereum2etl_resources/stages/verify/sqls/{task}.sql'.format(task=task))
        sql = read_file(sql_path)
        verify_task = BigQueryOperator(
            task_id='verify_{task}'.format(task=task),
            bql=sql,
            params=environment,
            use_legacy_sql=False,
            dag=dag)
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> verify_task
        return verify_task

    load_beacon_blocks_task = add_load_tasks('beacon_blocks', time_partitioning_field='block_timestamp')
    load_beacon_validators_task = add_load_tasks('beacon_validators', time_partitioning_field='timestamp')
    load_beacon_validators_hourly_task = add_load_tasks(
        'beacon_validators_hourly',
        time_partitioning_field='timestamp',
        file_name='beacon_validators')
    load_beacon_validators_latest_task = add_load_tasks(
        'beacon_validators_latest',
        time_partitioning_field='timestamp',
        wait_load_task='export_beacon_validators_hourly',
        file_name='beacon_validators')
    load_beacon_committees_task = add_load_tasks('beacon_committees', time_partitioning_field='epoch_timestamp')

    verify_blocks_count_task = add_verify_tasks('blocks_count', dependencies=[load_beacon_blocks_task])
    verify_blocks_have_latest_task = add_verify_tasks('blocks_have_latest', dependencies=[load_beacon_blocks_task])
    # verify_validators_count_task = add_verify_tasks('validators_count', dependencies=[load_beacon_validators_task])
    verify_committees_count_task = add_verify_tasks('committees_count', dependencies=[load_beacon_committees_task])

    return dag
