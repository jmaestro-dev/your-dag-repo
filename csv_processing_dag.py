from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Define the volume and volume mount for the pod
# This uses the PVC you created in the prerequisites.
volume = k8s.V1Volume(
    name="csv-data-storage",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="csv-data-pvc"),
)

volume_mount = k8s.V1VolumeMount(
    name="csv-data-storage",
    mount_path="/data", # The directory inside the pod where the volume will be mounted
    sub_path=None,
    read_only=False,
)

with DAG(
    dag_id="csv_processor_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None, # This DAG runs only when manually triggered
    catchup=False,
    tags=["csv", "kubernetes"],
) as dag:
    run_parent_app_task = KubernetesPodOperator(
        task_id="run_parent_app_processor",
        namespace="csv-processor", # Namespace where your services are running
        # --- IMPORTANT: Change this to your image ---
        image="jmaestroqontigo/parent-app:latest",
        name="airflow-parent-app-pod", # Name for the pod created by this task
        cmds=["python"],
        arguments=["main.py"], # Assumes your parent app's entrypoint is main.py
        # Mount the PVC into the pod at the /data directory
        volumes=[volume],
        volume_mounts=[volume_mount],
        # Stream the pod's logs to the Airflow task logs for easy debugging
        get_logs=True,
        # Clean up the pod after the task is finished
        is_delete_operator_pod=True,
        # Ensure the pod pulls the latest image tag every time
        image_pull_policy="Always",
    )
