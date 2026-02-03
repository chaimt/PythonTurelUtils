#!/usr/bin/env bash
#source $PWD/bi-core/env/bin/activate

export AIRFLOW_HOME=$(dirname "$PWD")/target
rm -rf $AIRFLOW_HOME
mkdir $AIRFLOW_HOME
echo
echo "******** AIRFLOW_HOME = $AIRFLOW_HOME ***********"

airflow initdb

sed 's|USER_DIR|'$(dirname "$PWD")'|g' airflow.cfg > $AIRFLOW_HOME/airflow.cfg

echo
echo "******** adding variables ***********"
airflow variables -s TEST_MODE True
airflow variables -s TEST_ENV dev
airflow variables -s TEST_DAG 'cancel_dataflow_jobs'
airflow variables -s BQ_JOBS_BUCKET 'my-bucket'


echo
echo "******** adding connections ***********"
echo "adding connections"
airflow connections -a \
  --conn_id=bigquery_bi_dev \
  --conn_type=google_cloud_platform \
  --conn_extra='{ "extra__google_cloud_platform__key_path": "'$PWD'/gbq-bigquery-bi-dev.json", "extra__google_cloud_platform__project": "dev", "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform"}'

airflow connections -a \
  --conn_id=google_cloud_dev \
  --conn_type=google_cloud_platform \
  --conn_extra='{ "extra__google_cloud_platform__key_path": "'$PWD'/gbq-bigquery-bi-dev.json", "extra__google_cloud_platform__project": "dev", "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform"}'


airflow connections -a \
  --conn_id=system_slack \
  --conn_type=google_cloud_platform
