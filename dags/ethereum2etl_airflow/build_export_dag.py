from __future__ import print_function

import os
import logging
from datetime import timedelta
from tempfile import TemporaryDirectory

from airflow import DAG, configuration
from airflow.operators import python_operator

from ethereum2etl.cli import (
    get_block_range_for_date,
    get_epoch_range_for_date,
    export_beacon_blocks,
    export_beacon_validators, export_beacon_committees)

from ethereum2etl_airflow.gcs_utils import upload_to_gcs


def build_export_dag(
        dag_id,
        provider_uris,
        output_bucket,
        export_start_date,
        export_rate_limit=None,
        export_end_date=None,
        notification_emails=None,
        export_schedule_interval='0 0 * * *',
        export_max_workers=5,
        export_max_active_runs=None,
):
    """Build Export DAG"""
    default_dag_args = {
        "depends_on_past": False,
        "start_date": export_start_date,
        "end_date": export_end_date,
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 5,
        "retry_delay": timedelta(minutes=5)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    if export_max_active_runs is None:
        export_max_active_runs = configuration.conf.getint('core', 'max_active_runs_per_dag')

    dag = DAG(
        dag_id,
        schedule_interval=export_schedule_interval,
        default_args=default_dag_args,
        max_active_runs=export_max_active_runs,
        concurrency=1,
    )

    from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
    cloud_storage_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id="google_cloud_default")

    # Export
    def export_path(directory, date):
        return "export/{directory}/block_date={block_date}/".format(
            directory=directory, block_date=date.strftime("%Y-%m-%d")
        )

    def copy_to_export_path(file_path, export_path):
        logging.info('Calling copy_to_export_path({}, {})'.format(file_path, export_path))
        filename = os.path.basename(file_path)

        upload_to_gcs(
            gcs_hook=cloud_storage_hook,
            bucket=output_bucket,
            object=export_path + filename,
            filename=file_path)

    def get_block_range(tempdir, date, provider_uri):
        logging.info('Calling get_block_range_for_date({},{}, ...)'.format(date, provider_uri))
        get_block_range_for_date.callback(
            date=date,
            output=os.path.join(tempdir, "blocks_meta.txt"),
            provider_uri=provider_uri,
            rate_limit=export_rate_limit)

        with open(os.path.join(tempdir, "blocks_meta.txt")) as block_range_file:
            block_range = block_range_file.read()
            start_block, end_block = block_range.split(",")

        return int(start_block), int(end_block)

    def get_epoch_range(tempdir, date, provider_uri):
        logging.info('Calling get_epoch_range_for_date({},{}, ...)'.format(date, provider_uri))
        get_epoch_range_for_date.callback(
            date=date,
            output=os.path.join(tempdir, "epochs_meta.txt"),
            provider_uri=provider_uri,
            rate_limit=export_rate_limit)

        with open(os.path.join(tempdir, "epochs_meta.txt")) as epoch_range_file:
            epoch_range = epoch_range_file.read()
            start_epoch, end_epoch = epoch_range.split(",")

        return int(start_epoch), int(end_epoch)

    def export_beacon_blocks_command(execution_date, provider_uri, **kwargs):
        with TemporaryDirectory() as tempdir:
            start_block, end_block = get_block_range(tempdir, execution_date, provider_uri)

            logging.info('Calling export_beacon_blocks({}, {}, {}, {}, {})'.format(
                start_block, end_block, provider_uri, export_max_workers, tempdir))

            export_beacon_blocks.callback(
                start_block=start_block,
                end_block=end_block,
                provider_uri=provider_uri,
                rate_limit=export_rate_limit,
                max_workers=export_max_workers,
                output_dir=tempdir,
                output_format='json'
            )

            copy_to_export_path(
                os.path.join(tempdir, "blocks_meta.txt"), export_path("blocks_meta", execution_date)
            )

            copy_to_export_path(
                os.path.join(tempdir, "beacon_blocks.json"), export_path("beacon_blocks", execution_date)
            )

    def export_beacon_validators_command(execution_date, provider_uri, **kwargs):
        with TemporaryDirectory() as tempdir:

            logging.info('Calling export_beacon_validators({}, {}, {})'.format(
                provider_uri, export_max_workers, tempdir))

            export_beacon_validators.callback(
                start_epoch=None,
                end_epoch=None,
                provider_uri=provider_uri,
                rate_limit=export_rate_limit,
                max_workers=export_max_workers,
                output_dir=tempdir,
                output_format='json'
            )

            copy_to_export_path(
                os.path.join(tempdir, "beacon_validators.json"), export_path("beacon_validators", execution_date)
            )

    def export_beacon_committees_command(execution_date, provider_uri, **kwargs):
        with TemporaryDirectory() as tempdir:
            start_epoch, end_epoch = get_epoch_range(tempdir, execution_date, provider_uri)

            logging.info('Calling export_beacon_committees({}, {}, {}, {})'.format(
                start_epoch, end_epoch, provider_uri, export_max_workers, tempdir))

            export_beacon_committees.callback(
                start_epoch=start_epoch,
                end_epoch=end_epoch,
                provider_uri=provider_uri,
                rate_limit=export_rate_limit,
                max_workers=export_max_workers,
                output_dir=tempdir,
                output_format='json'
            )

            copy_to_export_path(
                os.path.join(tempdir, "beacon_committees.json"), export_path("beacon_committees", execution_date)
            )

    def add_export_task(toggle, task_id, python_callable, dependencies=None):
        if toggle:
            operator = python_operator.PythonOperator(
                task_id=task_id,
                python_callable=python_callable,
                provide_context=True,
                execution_timeout=timedelta(hours=48),
                dag=dag,
            )
            if dependencies is not None and len(dependencies) > 0:
                for dependency in dependencies:
                    if dependency is not None:
                        dependency >> operator
            return operator
        else:
            return None

    # Operators

    add_export_task(
        True,
        "export_beacon_blocks",
        add_provider_uri_fallback_loop(export_beacon_blocks_command, provider_uris),
    )

    add_export_task(
        True,
        "export_beacon_validators",
        add_provider_uri_fallback_loop(export_beacon_validators_command, provider_uris),
    )

    add_export_task(
        True,
        "export_beacon_committees",
        add_provider_uri_fallback_loop(export_beacon_committees_command, provider_uris),
    )

    return dag


def add_provider_uri_fallback_loop(python_callable, provider_uris):
    """Tries each provider uri in provider_uris until the command succeeds"""
    def python_callable_with_fallback(**kwargs):
        for index, provider_uri in enumerate(provider_uris):
            kwargs['provider_uri'] = provider_uri
            try:
                python_callable(**kwargs)
                break
            except Exception as e:
                if index < (len(provider_uris) - 1):
                    logging.exception('An exception occurred. Trying another uri')
                else:
                    raise e

    return python_callable_with_fallback
