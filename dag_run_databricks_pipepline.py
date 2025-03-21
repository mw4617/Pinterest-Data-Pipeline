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
    config = load_config()
    DATABRICKS_INSTANCE = config["databricks"]["instance"]
    DATABRICKS_TOKEN = config["databricks"]["token"]

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
            raise Exception(f"API response did not return a run_id for job {job_id}: {response_json}")

        print(f"Databricks job {job_id} triggered successfully with run_id: {run_id}")

        # Push run_id to XCom
        ti = kwargs["ti"]
        ti.xcom_push(key=f"run_id_{job_name}", value=run_id)
        return run_id
    else:
        raise Exception(f"Failed to trigger Databricks job {job_id}: {response.text}")

def get_run_id_from_xcom(ti, task_id: str, key: str, retries: int = 3, delay: int = 10) -> int:
    """
    Attempt to retrieve the run_id from XCom with retries.

    Args:
        ti: Task instance from Airflow context.
        task_id (str): The task ID to pull from.
        key (str): The XCom key to pull.
        retries (int): Number of retry attempts.
        delay (int): Delay in seconds between retries.

    Returns:
        int: The retrieved run_id.

    Raises:
        Exception: If run_id is not found after retries.
    """
    for attempt in range(retries):
        run_id = ti.xcom_pull(task_ids=task_id, key=key)
        if run_id:
            return run_id
        print(f"[XCom] Attempt {attempt + 1}: run_id not found. Retrying in {delay}s...")
        time.sleep(delay)

    raise Exception(f"[XCom] Failed to retrieve run_id from task {task_id} after {retries} attempts.")

def check_job_status(run_id: int, status_response: dict) -> bool:
    """
    Check the Databricks job status and determine if it’s complete, failed, or still running.

    Args:
        run_id (int): Databricks run ID.
        status_response (dict): JSON response from the job status API.

    Returns:
        bool: True if the job is still running, False if it is complete.

    Raises:
        Exception: If the job failed or was cancelled.
    """
    state = status_response.get("state", {})
    life_cycle = state.get("life_cycle_state")
    result_state = state.get("result_state")

    print(f"[Databricks] run_id={run_id} | life_cycle={life_cycle} | result_state={result_state}")

    if life_cycle == "TERMINATED":
        if result_state == "SUCCESS":
            print(f"[Databricks] Job {run_id} completed successfully.")
            return False
        else:
            raise Exception(f"[Databricks] Job {run_id} failed with result state: {result_state}")
    elif life_cycle in ["INTERNAL_ERROR", "SKIPPED", "FAILED"]:
        raise Exception(f"[Databricks] Job {run_id} failed with life cycle state: {life_cycle}")

    return True  # Job still running

def poll_databricks_job_status(run_id: int, databricks_instance: str, databricks_token: str, poll_interval: int = 10, timeout: int = 1800) -> None:
    """
    Poll Databricks job status until it finishes or fails.

    Args:
        run_id (int): Databricks run ID.
        databricks_instance (str): Databricks workspace URL.
        databricks_token (str): API token.
        poll_interval (int): Seconds between polling intervals.
        timeout (int): Max seconds to wait before raising a timeout.

    Raises:
        TimeoutError: If job takes too long to complete.
        Exception: If job fails or an API error occurs.
    """
    url = f"{databricks_instance}/api/2.1/jobs/runs/get?run_id={run_id}"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }

    start_time = time.time()

    while True:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"[Databricks] Failed to fetch status for run_id {run_id}: {response.text}")

        status_json = response.json()

        if not check_job_status(run_id, status_json):
            return  # Job completed

        if time.time() - start_time > timeout:
            raise TimeoutError(f"[Databricks] Job {run_id} timed out after {timeout} seconds.")

        time.sleep(poll_interval)

def wait_for_databricks_job(job_name: str, job_id: int, **kwargs) -> None:
    """
    Poll the Databricks API to check the status of a running job.

    Args:
        job_name (str): Name of the Databricks job.
        job_id (int): Databricks job ID.

    Raises:
        Exception: If the job fails, times out, or encounters an API error.
    """
    ti = kwargs["ti"]
    config = load_config()
    DATABRICKS_INSTANCE = config["databricks"]["instance"]
    DATABRICKS_TOKEN = config["databricks"]["token"]

    run_id = get_run_id_from_xcom(
        ti=ti,
        task_id=f"trigger_load_transform_{job_name}_job",
        key=f"run_id_{job_name}"
    )

    poll_databricks_job_status(
        run_id=run_id,
        databricks_instance=DATABRICKS_INSTANCE,
        databricks_token=DATABRICKS_TOKEN
    )

# Task creation
tasks = {}

for job_name, job_id in JOB_IDS.items():
    trigger_task_id = f"trigger_load_transform_{job_name}_job"
    wait_task_id = f"wait_for_load_transform_{job_name}_job"

    tasks[trigger_task_id] = PythonOperator(
        task_id=trigger_task_id,
        python_callable=trigger_databricks_job,
        op_kwargs={"job_name": job_name, "job_id": job_id},
        dag=dag,
    )

    if job_name != "analytics":
        tasks[wait_task_id] = PythonOperator(
            task_id=wait_task_id,
            python_callable=wait_for_databricks_job,
            op_kwargs={"job_name": job_name, "job_id": job_id},
            dag=dag,
        )

# Set DAG dependencies
tasks["trigger_load_transform_geo_data_job"] >> tasks["wait_for_load_transform_geo_data_job"]
tasks["trigger_load_transform_pinterest_data_job"] >> tasks["wait_for_load_transform_pinterest_data_job"]
tasks["trigger_load_transform_user_data_job"] >> tasks["wait_for_load_transform_user_data_job"]

# Final job depends on all prior waits
[
    tasks["wait_for_load_transform_user_data_job"],
    tasks["wait_for_load_transform_geo_data_job"],
    tasks["wait_for_load_transform_pinterest_data_job"],
] >> tasks["trigger_load_transform_analytics_job"]

