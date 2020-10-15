# Ethereum 2.0 ETL Airflow

Airflow DAGs for exporting and loading the Ethereum 2.0 blockchain data to Google BigQuery.
Data is available for you to query right away in [Google BigQuery](https://console.cloud.google.com/bigquery?page=dataset&d=crypto_ethereum2_medalla&p=public-data-finance).

## Prerequisites

* linux/macos terminal 
* git
* [gcloud](https://cloud.google.com/sdk/install)

## Setting Up

1. Create a GCS bucket to hold export files:

    ```bash
    gcloud config set project <your_gcp_project>
    PROJECT=$(gcloud config get-value project 2> /dev/null)
    ENVIRONMENT_INDEX=0
    BUCKET=${PROJECT}-${ENVIRONMENT_INDEX}
    gsutil mb gs://${BUCKET}/
    ```

2. Create a Google Cloud Composer environment:

    ```bash
    ENVIRONMENT_NAME=${PROJECT}-${ENVIRONMENT_INDEX} && echo "Environment name is ${ENVIRONMENT_NAME}"
    gcloud composer environments create ${ENVIRONMENT_NAME} --location=us-central1 --zone=us-central1-a \
        --disk-size=30GB --machine-type=custom-1-4096 --node-count=3 --python-version=3 --image-version=composer-1.8.3-airflow-1.10.3 \
        --network=default --subnetwork=default
    
    gcloud composer environments update $ENVIRONMENT_NAME --location=us-central1 --update-pypi-package=ethereum2-etl==0.0.4
    ```
   
    Note that if Composer API is not enabled the command above will auto prompt to enable it.

3. Follow the steps in [Configuring Airflow Variables](#configuring-airflow-variables) to configure Airfow variables.
    
4. Follow the steps in [Deploying Airflow DAGs](#deploying-airflow-dags) 
to deploy Airflow DAGs to Cloud Composer Environment.
 
5. Follow the steps [here](https://cloud.google.com/composer/docs/how-to/managing/creating#notification) 
to configure email notifications.

## Configuring Airflow Variables

- For a new environment clone Ethereum 2.0 ETL Airflow: `git clone https://github.com/blockchain-etl/ethereum2-etl-airflow && cd ethereum2-etl-airflow`. 
  For an existing environment use the `airflow_variables.json` file from 
  [Cloud Source Repository](#creating-a-cloud-source-repository-for-airflow-variables) for your environment.
- Edit `airflow_variables.json` and update configuration options with your values. 
  You can find variables description in the table below. For the `mainnet_output_bucket` variable 
  specify the bucket created on step 1 above. You can get it by running `echo $BUCKET`.
- Open Airflow UI. You can get its URL from `airflowUri` configuration option: 
  `gcloud composer environments describe ${ENVIRONMENT_NAME} --location us-central1`.
- Navigate to **Admin > Variables** in the Airflow UI, click **Choose File**, select `airflow_variables.json`, 
  and click **Import Variables**.
  
### Airflow Variables

Note that the variable names must be prefixed with `{chain}_`, e.g. `mainnet_output_bucket`. 

| Variable | Description |
|---|---|
| `output_bucket` | GCS bucket where exported files with blockchain data will be stored |
| `export_start_date` | export start date, default: `2018-06-30` |
| `export_end_date` | export end date, used for integration testing, default: None |
| `export_schedule_interval` | export cron schedule, default: `0 1 * * *` |
| `provider_uris` | comma-separated list of provider URIs for [ethereum2etl](https://ethereum2-etl.readthedocs.io/en/latest/commands) commands |
| `notification_emails` | comma-separated list of emails where notifications on DAG failures, retries and successes will be delivered. This variable must not be prefixed with `{chain}_` |
| `export_max_active_runs` | max active DAG runs for export, default: `3` |
| `export_max_workers` | max workers for [ethereum2etl](https://ethereum2-etl.readthedocs.io/en/latest/commands) command, default: `5` |
| `destination_dataset_project_id` | GCS project id where destination BigQuery dataset is |
| `load_schedule_interval` | load cron schedule, default: `0 2 * * *` |
| `load_end_date` | load end date, used for integration testing, default: None |

### Creating a Cloud Source Repository for Airflow variables

It is recommended to keep airflow_variables.json in a version control system e.g. git. 
Below are the commands for creating a Cloud Source Repository to hold airflow_variables.json: 

```bash
REPO_NAME=${PROJECT}-airflow-config-${ENVIRONMENT_INDEX} && echo "Repo name ${REPO_NAME}"
gcloud source repos create ${REPO_NAME}
gcloud source repos clone ${REPO_NAME} && cd ${REPO_NAME}

# Put airflow_variables.json to the root of the repo

git add airflow_variables.json && git commit -m "Initial commit"
git push

# TODO: Setup Cloud Build Trigger to deploy variables to Composer environment when updated. For now it has to be done manually.
```
  
## Deploying Airflow DAGs

- Get the value from `dagGcsPrefix` configuration option from the output of:
  `gcloud composer environments describe ${ENVIRONMENT_NAME} --location us-central1`.
- Upload DAGs to the bucket. Make sure to replace `<dag_gcs_prefix>` with the value from the previous step:
  `./upload_dags.sh <dag_gcs_prefix>`.
- To understand more about how the Airflow DAGs are structured 
  read [this article](https://cloud.google.com/blog/products/data-analytics/ethereum-bigquery-how-we-built-dataset).
- Note that it will take one or more days for `mainnet_export_dag` to finish exporting the historical data.
- To setup automated deployment of DAGs refer to [Cloud Build Configuration](/docs/cloudbuild-configuration.md).

## Integration Testing

It is [recommended](https://cloud.google.com/composer/docs/how-to/using/testing-dags#faqs_for_testing_workflows) to use a dedicated Cloud Composer
environment for integration testing with Airflow.

To run integration tests:
 
- Create a new environment following the steps in the [Setting Up](#setting-up) section.
- On the [Configuring Airflow Variables](#configuring-airflow-variables) step specify the following additional configuration variables:
    - `export_end_date`: `2018-06-30`
    - `load_end_date`: `2018-06-30`
- This will run the DAGs only for the first day. At the end of the load DAG the verification tasks will ensure
the correctness of the result.

## Troubleshooting

To troubleshoot issues with Airflow tasks use **View Log** button in the Airflow console for individual tasks.
Read [Airflow UI overview](https://airflow.apache.org/docs/stable/ui.html) and 
[Troubleshooting DAGs](https://cloud.google.com/composer/docs/how-to/using/troubleshooting-dags) for more info. 
 
In rare cases you may need to inspect GKE cluster logs in 
[GKE console](https://console.cloud.google.com/kubernetes/workload). 

**Speed up the initial export**

To speed up the initial data export it is recommended to use `n1-standard-2` instance type for the Cloud Composer cluster.
After the initial export is finished a new cluster with `custom-1-4096` should be created with `export_start_date`
Airflow variable set to the previous date.