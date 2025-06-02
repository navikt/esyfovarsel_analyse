

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from dataverk_airflow import quarto_operator
from kubernetes import client as k8s

with DAG('narmesteleder_dag', start_date = days_ago(1) , schedule_interval="0 4 * * *") as dag:
    t1 = quarto_operator(dag=dag,
                         name="narmesteleder_dag",
                         repo="navikt/esyfovarsel_analyse",
                         quarto={
                            "path": "narmesteleder.py",
                            "env": "prod",
                            "id": "97129fb6-fa1b-4b3e-830b-0c5d1b768e4c",
                            "token": Variable.get("TEAMSYKEFRAVR_TOKEN"),
                        },
                         requirements_path="requirements.txt",
                         use_uv_pip_install=True,
                         resources=k8s.V1ResourceRequirements(
                            requests={"ephemeral-storage": "1Gi"},
                        ),
                         slack_channel="#syfortellinger-alert",
                         python_version=Variable.get('PYTHON_VERSION'),
                         retries=0,
                         startup_timeout_seconds=600,
                         )                            
    

    t1