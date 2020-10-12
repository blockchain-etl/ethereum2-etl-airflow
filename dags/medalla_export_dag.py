from __future__ import print_function

from ethereum2etl_airflow.build_export_dag import build_export_dag
from ethereum2etl_airflow.variables import read_export_dag_vars

# airflow DAG
DAG = build_export_dag(
    dag_id='medalla_export_dag',
    **read_export_dag_vars(
        var_prefix='medalla_',
        export_schedule_interval='0 1 * * *',
        export_start_date='2020-08-04',
        export_max_active_runs=1,
        export_max_workers=5,
    )
)
