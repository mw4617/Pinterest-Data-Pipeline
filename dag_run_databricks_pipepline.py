"""
Airflow DAG to trigger Databricks jobs sequentially and monitor their status.

This DAG:
- Loads Databricks credentials from a secure YAML file.
- Triggers Databricks jobs one after another.
- Waits for each job to complete before proceeding.
- Uses XCom to pass run IDs between tasks.
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import time
import yaml
import os

# Load configuration from YAML file
def load_config():
    """
    Load Databricks instance and token from a secure YAML file.

    Returns:
        config (dict): A dictionary containing Databricks API credentials.

    Raises:
        FileNotFoundError: If the `config.yaml` file is missing.
        yaml.YAMLError: If there is an issue parsing the YAML file.
    """
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

# Load config values
config = load_config()
DATABRICKS_INSTANCE = config["databricks"]["instance"]
DATABRICKS_TOKEN = config["databricks"]["token"]

# Default arguments for DAG execution
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),  
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,  
    "retry_delay": timedelta(minutes=2),  
}

# Define DAG
dag = DAG(
    "databricks_run_pinterest_analytics_pipeline_dag",
    default_args=default_args,
    description="Run Databricks jobs sequentially and wait for completion",
    schedule_interval=None,  
    catchup=False,  
)

# Databricks Job IDs
JOB_IDS = {
    "geo_data": 870513265362243,
    "pinterest_data": 1018658494325464,
    "user_data": 267770584373278,
    "analytics": 315681854485654,
}

def trigger_databricks_job(job_name: str, job_id: int, **kwargs) -> int:
    """
    Trigger a Databricks job and store the run ID in XCom.

    Args:
        job_name (str): Name of the Databricks job.
        job_id (int): Databricks job ID.

    Returns:
        run_id (int): The run ID of the triggered Databricks job.

    Raises:
        Exception: If the job fails to trigger or response does not contain a `run_id`.
    """
    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"job_id": job_id}

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        response_json = response.json()
        run_id = response_json.get("run_id")

        if not run_id:
            raise Exception(f"❌ API response did not return a run_id for job {job_id}: {response_json}")

        print(f"✅ Databricks job {job_id} triggered successfully with run_id: {run_id}")

        # Push run_id to XCom
        ti = kwargs["ti"]
        ti.xcom_push(key=f"run_id_{job_name}", value=run_id)
        return run_id
    else:
        raise Exception(f"❌ Failed to trigger Databricks job {job_id}: {response.text}")

def wait_for_databricks_job(job_name: str, job_id: int, **kwargs) -> None:
    """
    Poll the Databricks API to check the status of a running job.

    Args:
        job_name (str): Name of the Databricks job.
        job_id (int): Databricks job ID.

    Returns:
        None

    Raises:
        Exception: If the job fails, times out, or encounters an API error.
    """
    ti = kwargs["ti"]

    # Retry pulling run_id from XCom (max 3 attempts)
    for attempt in range(3):
        run_id = ti.xcom_pull(task_ids=f"trigger_load_transform_{job_name}_job", key=f"run_id_{job_name}")
        if run_id:
            break
        print(f"⚠️ Attempt {attempt + 1}: No run_id found for job {job_id}. Retrying in 10s...")
        time.sleep(10)

    if not run_id:
        raise Exception(f"❌ No run_id found for job {job_id}. Check if XCom push was successful.")

    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/runs/get?run_id={run_id}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
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

# Creating task operators dynamically for efficiency
tasks = {}

for job_name, job_id in JOB_IDS.items():
    trigger_task_id = f"trigger_load_transform_{job_name}_job"
    wait_task_id = f"wait_for_load_transform_{job_name}_job"

    # Task to trigger Databricks job
    tasks[trigger_task_id] = PythonOperator(
        task_id=trigger_task_id,
        python_callable=trigger_databricks_job,
        op_kwargs={"job_name": job_name, "job_id": job_id},
        dag=dag,
    )

    # Task to wait for Databricks job completion (except for the final analytics job)
    if job_name != "analytics":
        tasks[wait_task_id] = PythonOperator(
            task_id=wait_task_id,
            python_callable=wait_for_databricks_job,
            op_kwargs={"job_name": job_name, "job_id": job_id},
            dag=dag,
        )

# Define dependencies between tasks
tasks["trigger_load_transform_geo_data_job"] >> tasks["wait_for_load_transform_geo_data_job"]
tasks["trigger_load_transform_pinterest_data_job"] >> tasks["wait_for_load_transform_pinterest_data_job"]
tasks["trigger_load_transform_user_data_job"] >> tasks["wait_for_load_transform_user_data_job"]

# Join all wait tasks before executing the final analytics job
[
    tasks["wait_for_load_transform_user_data_job"],
    tasks["wait_for_load_transform_geo_data_job"],
    tasks["wait_for_load_transform_pinterest_data_job"],
] >> tasks["trigger_load_transform_analytics_job"]
