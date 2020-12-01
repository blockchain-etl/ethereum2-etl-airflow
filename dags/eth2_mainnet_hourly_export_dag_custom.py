from __future__ import print_function

from ethereum2etl_airflow.build_hourly_export_dag_custom import build_hourly_export_dag_custom
from ethereum2etl_airflow.variables import read_export_dag_vars

# airflow DAG
DAG = build_hourly_export_dag_custom(
    dag_id='eth2_mainnet_hourly_export_dag_custom',
    **read_export_dag_vars(
        var_prefix='eth2_mainnet_',
        export_schedule_interval='30 * * * *',
        export_start_date='2020-12-01',
        export_max_active_runs=1,
        export_max_workers=5,
    )
)
