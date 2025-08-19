
import sys
import re
import os
import pandas_gbq
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.trigger_rule import TriggerRule
from kubernetes import client as k8s


def get_n_rows_yesterday(**context):
    project = 'teamsykefravr-prod-7e29'
    sql = 'SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT utsendt_forsok_tidspunkt FROM utsending_varsel_feilet where utsendt_forsok_tidspunkt > CURRENT_DATE - 3 and utsendt_forsok_tidspunkt < CURRENT_DATE;")'

    df = pandas_gbq.read_gbq(sql, project_id=project)
    count = len(df)
    context['ti'].xcom_push(key='row_count', value=count)
    return count


def skal_sendes_slack(**context):
    count = context['ti'].xcom_pull(task_ids='varsel_status', key='row_count')
    return count > 0

with DAG(
    dag_id='overvakning',
    schedule_interval="0 15 * * *",
    start_date=datetime(2025, 7, 10),
    catchup=False,
    default_args={"retries": 1},
) as dag:

  
    t_feilet_status = PythonOperator(
        task_id='varsel_status',
        python_callable=get_n_rows_yesterday,
        provide_context=True,
    )

   
    t_sjekk_send = ShortCircuitOperator(
        task_id='sjekk_send_slack',
        python_callable=skal_sendes_slack,
        provide_context=True,
    )

    
    t_varsling = SlackAPIPostOperator(
        task_id="varsling",
        slack_conn_id="slack_connection",
        channel="#esyfo-data-alert",
        text = ("TEST: NB! Nye rader i `esyfovarsel.utsendt_varsel_feilet` i går. "
    "Antall rader: {{ ti.xcom_pull(task_ids='varsel_status', key='row_count') }}\n"
    "@esyfo_utviklere"),
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
            )
        },
    )

    slack_ved_feil = SlackAPIPostOperator(
        task_id='slack_ved_feil',
        slack_conn_id='slack_connection',
        channel='#syfortellinger-alert',
        text=":error: *Noe feilet i DAG `overvakning`!* Se Airflow for detaljer.",
        trigger_rule=TriggerRule.ONE_FAILED,  # Kjører hvis en eller flere upstream feiler
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
            )
        },
    )

    t_stop = EmptyOperator(task_id="stop_task")

   
    t_feilet_status >> [t_sjekk_send, slack_ved_feil]
    t_sjekk_send >> t_varsling >> t_stop

    for task in [t_sjekk_send, t_varsling, t_stop]:
        task >> slack_ved_feil
