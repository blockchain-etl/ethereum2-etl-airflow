from __future__ import print_function

import logging

from ethereum2etl_airflow.build_load_dag import build_load_dag
from ethereum2etl_airflow.variables import read_load_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# airflow DAG
DAG = build_load_dag(
    dag_id='medalla_load_dag',
    chain='ethereum2',
    **read_load_dag_vars(
        var_prefix='medalla_',
        load_schedule_interval='0 2 * * *'
    )
)
