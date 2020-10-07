from __future__ import print_function

import logging
import os
from datetime import datetime, timedelta

from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning

from ethereum2etl_airflow.bigquery_utils import submit_bigquery_job, create_dataset, read_bigquery_schema_from_file, \
    create_view
from ethereum2etl_airflow.file_utils import read_file

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_load_dag(
        dag_id,
        output_bucket,
        destination_dataset_project_id,
        chain='ethereum2',
        notification_emails=None,
        load_start_date=datetime(2018, 6, 30),
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
        'email_on_retry': True,
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

    def add_seed_tasks(task):
        def seed_task():
            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig()
            schema_path = os.path.join(dags_folder, 'resources/stages/seed/schemas/{task}.json'.format(task=task))
            job_config.schema = read_bigquery_schema_from_file(schema_path)
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.compression = bigquery.Compression.GZIP
            job_config.write_disposition = 'WRITE_TRUNCATE'
            job_config.ignore_unknown_values = True

            file_path = os.path.join(dags_folder, 'resources/stages/seed/data/{task}.json.gz'.format(task=task))
            table_ref = create_dataset(client, dataset_name, destination_dataset_project_id).table(task)
            load_job = client.load_table_from_file(open(file_path, mode='r+b'), table_ref, job_config=job_config)
            submit_bigquery_job(load_job, job_config)
            assert load_job.state == 'DONE'

        seed_operator = PythonOperator(
            task_id='seed_{task}'.format(task=task),
            python_callable=seed_task,
            execution_timeout=timedelta(minutes=30),
            dag=dag
        )

        return seed_operator

    def add_load_tasks(task, time_partitioning_field='timestamp'):
        wait_sensor = GoogleCloudStorageObjectSensor(
            task_id='wait_latest_{task}'.format(task=task),
            timeout=60 * 60,
            poke_interval=60,
            bucket=output_bucket,
            object='export/{task}/block_date={datestamp}/{task}.json'.format(task=task, datestamp='{{ds}}'),
            dag=dag
        )

        def load_task():
            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig()
            schema_path = os.path.join(dags_folder, 'resources/stages/load/schemas/{task}.json'.format(task=task))
            job_config.schema = read_bigquery_schema_from_file(schema_path)
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.write_disposition = 'WRITE_TRUNCATE'
            job_config.ignore_unknown_values = True
            job_config.time_partitioning = TimePartitioning(field=time_partitioning_field)

            export_location_uri = 'gs://{bucket}/export'.format(bucket=output_bucket)
            uri = '{export_location_uri}/{task}/*.json'.format(export_location_uri=export_location_uri, task=task)
            table_ref = create_dataset(client, dataset_name, destination_dataset_project_id).table(task)
            load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
            submit_bigquery_job(load_job, job_config)
            assert load_job.state == 'DONE'

        load_operator = PythonOperator(
            task_id='load_{task}'.format(task=task),
            python_callable=load_task,
            execution_timeout=timedelta(minutes=30),
            dag=dag
        )

        wait_sensor >> load_operator
        return load_operator

    def add_create_operations_view_tasks(dependencies=None):
        def create_view_task(ds, **kwargs):

            template_context = kwargs.copy()
            template_context['ds'] = ds
            template_context['params'] = environment

            client = bigquery.Client()

            dest_table_name = 'operations'
            dest_table_ref = create_dataset(client, dataset_name, project=destination_dataset_project_id).table(dest_table_name)

            sql_path = os.path.join(dags_folder, 'resources/stages/load/sqls/operations.sql')
            sql_template = read_file(sql_path)
            sql = kwargs['task'].render_template('', sql_template, template_context)
            print('operations view: \n' + sql)

            create_view(client, sql, dest_table_ref)

        create_view_operator = PythonOperator(
            task_id='create_operations_view',
            python_callable=create_view_task,
            provide_context=True,
            execution_timeout=timedelta(minutes=60),
            dag=dag
        )

        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> create_view_operator
        return create_view_operator

    def add_verify_tasks(task, dependencies=None):
        # The queries in verify/sqls will fail when the condition is not met
        # Have to use this trick since the Python 2 version of BigQueryCheckOperator doesn't support standard SQL
        # and legacy SQL can't be used to query partitioned tables.
        sql_path = os.path.join(dags_folder, 'resources/stages/verify/sqls/{task}.sql'.format(task=task))
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

    add_seed_tasks('migrations')

    all_tables = ['blocks', 'balance_updates']

    load_tasks = {}
    for table in all_tables:
        load_task = add_load_tasks(table)
        load_tasks[table] = load_task

    create_operations_view_task = add_create_operations_view_tasks(dependencies=load_tasks.values())

    verify_blocks_count_task = add_verify_tasks('blocks_count', dependencies=[load_tasks['blocks']])
    verify_blocks_have_latest_task = add_verify_tasks('blocks_have_latest', dependencies=[load_tasks['blocks']])
    verify_operations_count_task = add_verify_tasks('operations_count',
                                                    dependencies=[load_tasks.values()] + [create_operations_view_task])

    if notification_emails and len(notification_emails) > 0:
        send_email_task = EmailOperator(
            task_id='send_email',
            to=[email.strip() for email in notification_emails.split(',')],
            subject='Ethereum2 ETL Airflow Load DAG Succeeded',
            html_content='Ethereum2 ETL Airflow Load DAG Succeeded - {}'.format(chain),
            dag=dag
        )
        verify_blocks_count_task >> send_email_task
        verify_blocks_have_latest_task >> send_email_task
        verify_operations_count_task >> send_email_task

    return dag
