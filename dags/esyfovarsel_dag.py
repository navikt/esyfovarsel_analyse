

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from dataverk_airflow import quarto_operator
from kubernetes import client as k8s

with DAG('esyfovarsel_dag', start_date = days_ago(1) , schedule_interval="0 4 * * *") as dag:
    t1 = quarto_operator(dag=dag,
                         name="esyfovarsel_dag",
                         repo="navikt/esyfovarsel_analyse",
                         quarto={
                            "path": "esyfovarsel_analyse/esyfovarsel.py",
                            "env": "prod",
                            "id": "f68dc288-8f02-4111-9ff9-39d4cc8d6168",
                            "token": Variable.get("TEAMSYKEFRAVR_TOKEN"),
                        },
                         requirements_path="requirements_airflow.txt",
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