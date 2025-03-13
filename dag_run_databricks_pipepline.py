from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import time

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,  # Retry 3 times before failing
    'retry_delay': timedelta(minutes=2),  # Wait 2 minutes before retrying
}

# Define DAG
dag = DAG(
    'databricks_run_pinterest_analytics_pipeline_dag',
    default_args=default_args,
    description='Run Databricks jobs sequentially and wait for completion',
    schedule_interval=None,
    catchup=False,
)

# Databricks API details
DATABRICKS_INSTANCE = "https://dbc-4e4e95da-0506.cloud.databricks.com"
DATABRICKS_TOKEN = "dapi9c994c7ded58e49c9f6b6481cf49b464"

# Job IDs
JOB_IDS = {
    "geo_data": 870513265362243,
    "pinterest_data": 1018658494325464,
    "user_data": 267770584373278,
    "analytics": 315681854485654
}

# Function to trigger a Databricks job and push its run ID to XCom
def trigger_databricks_job(job_name, job_id, **kwargs):
    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {"job_id": job_id}

    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        response_json = response.json()
        run_id = response_json.get("run_id")

        if not run_id:
            raise Exception(f"❌ API response did not return a run_id for job {job_id}: {response_json}")

        print(f"✅ Databricks job {job_id} triggered successfully with run_id: {run_id}")

        # Push run_id to XCom with correct key
        ti = kwargs['ti']
        ti.xcom_push(key=f'run_id_{job_name}', value=run_id)
        return run_id
    else:
        raise Exception(f"❌ Failed to trigger Databricks job {job_id}: {response.text}")

# Function to check the status of a Databricks job
def wait_for_databricks_job(job_name, job_id, **kwargs):
    ti = kwargs['ti']

    # Retry pulling run_id for up to 3 attempts
    for attempt in range(3):
        run_id = ti.xcom_pull(task_ids=f'trigger_load_transform_{job_name}_job', key=f'run_id_{job_name}')
        if run_id:
            break
        print(f"⚠️ Attempt {attempt + 1}: No run_id found for job {job_id}. Retrying in 10s...")
        time.sleep(10)
    
    if not run_id:
        raise Exception(f"❌ No run_id found for job {job_id}. Check if XCom push was successful.")

    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/runs/get?run_id={run_id}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    while True:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            job_status = response.json().get("state", {}).get("life_cycle_state")
            print(f"🔄 Job {run_id} status: {job_status}")

            if job_status in ["TERMINATED", "SUCCESS"]:
                print(f"✅ Job {run_id} completed.")
                return
            elif job_status in ["INTERNAL_ERROR", "SKIPPED", "FAILED"]:
                raise Exception(f"❌ Job {run_id} failed with status: {job_status}")

        else:
            raise Exception(f"❌ Failed to fetch job status for run_id {run_id}: {response.text}")

        time.sleep(10)  # Wait before checking again

# Creating task operators dynamically to avoid redundancy
tasks = {}

for job_name, job_id in JOB_IDS.items():
    trigger_task_id = f"trigger_load_transform_{job_name}_job"
    wait_task_id = f"wait_for_load_transform_{job_name}_job"

    tasks[trigger_task_id] = PythonOperator(
        task_id=trigger_task_id,
        python_callable=trigger_databricks_job,
        op_kwargs={'job_name': job_name, 'job_id': job_id},
        dag=dag,
    )

    if job_name != "analytics":  # No wait task for final analytics job
        tasks[wait_task_id] = PythonOperator(
            task_id=wait_task_id,
            python_callable=wait_for_databricks_job,
            op_kwargs={'job_name': job_name, 'job_id': job_id},
            dag=dag,
        )

# Define dependencies
tasks["trigger_load_transform_geo_data_job"] >> tasks["wait_for_load_transform_geo_data_job"]
tasks["trigger_load_transform_pinterest_data_job"] >> tasks["wait_for_load_transform_pinterest_data_job"]
tasks["trigger_load_transform_user_data_job"] >> tasks["wait_for_load_transform_user_data_job"]

# Join all wait tasks before final analytics job
[
    tasks["wait_for_load_transform_user_data_job"], 
    tasks["wait_for_load_transform_geo_data_job"], 
    tasks["wait_for_load_transform_pinterest_data_job"]
] >> tasks["trigger_load_transform_analytics_job"]
