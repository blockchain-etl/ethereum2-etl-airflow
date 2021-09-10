from __future__ import print_function

import logging

from ethereum2etl_airflow.build_hourly_load_dag import build_hourly_load_dag
from ethereum2etl_airflow.variables import read_load_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# airflow DAG
DAG = build_hourly_load_dag(
    dag_id='eth2_mainnet_hourly_load_dag',
    chain='ethereum2',
    **read_load_dag_vars(
        var_prefix='eth2_mainnet_',
        load_schedule_interval='30 * * * *'
    )
)
