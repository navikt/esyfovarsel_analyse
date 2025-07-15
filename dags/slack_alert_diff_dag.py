import os
import sys
import re
from airflow import DAG
import datetime as dt
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
import pandas_gbq

from kubernetes import client as k8s

def get_esyfo_n_rows_yesterday():
    
    project = 'teamsykefravr-prod-7e29'
    sql = """
    SELECT * FROM EXTERNAL_QUERY(
    "team-esyfo-prod-bbe6.europe-north1.esyfovarsel",
    "SELECT utsendt_tidspunkt, type, kanal FROM utsendt_varsel WHERE utsendt_tidspunkt > CURRENT_DATE - 1 AND utsendt_tidspunkt < CURRENT_DATE AND type = 'SM_DIALOGMOTE_INNKALT' AND kanal = 'DITT_SYKEFRAVAER';"
    )
    """
    df = pandas_gbq.read_gbq(sql, project_id=project)       

    return len(df)
  
def get_dialogmote_n_rows_yesterday():
    project = 'teamsykefravr-prod-7e29'

    sql = """
    SELECT * FROM EXTERNAL_QUERY(
    "teamsykefravr-prod-7e29.europe-north1.dialogmote",
    "SELECT cast(id as text), cast(uuid as text), created_at, updated_at, cast(mote_id as text), status, opprettet_av, tilfelle_start, published_at, motedeltaker_behandler FROM mote_status_endret WHERE created_at > CURRENT_DATE - 1 AND created_at < CURRENT_DATE  AND status = 'INNKALT';"
    )
    """
    df = pandas_gbq.read_gbq(sql, project_id=project)       

    return len(df)
    

def varsel_status(**context):
    len_esyfo = get_esyfo_n_rows_yesterday()
    len_dialogmote = get_dialogmote_n_rows_yesterday()
    diff = len_dialogmote - len_esyfo

    if len_esyfo < len_dialogmote:
        ret = 'varsling'
    else:
        ret = 'stop_task'

    context['ti'].xcom_push(key='diff', value=diff)
    context['ti'].xcom_push(key='status', value=ret)
    return ret
    

def sjekk_og_send_slack(**context):
    ti = context['ti']
    status = ti.xcom_pull(task_ids='varsel_status', key='status')
    diff = ti.xcom_pull(task_ids='varsel_status', key='diff')

    if status == 'varsling':
        # Returner meldingstekst som brukes i Slack-operator
        return f"NB! data differanse mellom esyfo-utsendt_varsel-SM_DIALOGMOTE_INNKALT og isyfo-dialogmote-INNKALT i går. Antall differanse: {diff}."
    else:
        # Returner None for å hoppe over Slack-meldingen
        return None

with DAG(
    'overvakning_diff',
    schedule_interval="0 4 * * *",
    start_date=datetime(2025, 7, 10),
    catchup=False,
) as dag:

    t_varsel_status = PythonOperator(
        task_id='varsel_status',
        python_callable=varsel_status,
        provide_context=True,
    )

    t_sjekk_slack_melding = PythonOperator(
        task_id='sjekk_og_send_slack',
        python_callable=sjekk_og_send_slack,
        provide_context=True,
    )

    t_send_slack = SlackAPIPostOperator(
        task_id='send_slack',
        slack_conn_id='slack_connection',
        channel='#syfortellinger-alert',
        text="{{ ti.xcom_pull(task_ids='sjekk_og_send_slack') }}",
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
            )
        },
        trigger_rule='all_success',  # kjør kun hvis sjekk_og_send_slack lykkes
    )

    t_stop = EmptyOperator(task_id='stop_task')

    # Workflow:
    # kjør varsel_status -> sjekk om slackmelding skal sendes -> send slackmelding -> stopp
    t_varsel_status >> t_sjekk_slack_melding >> t_send_slack >> t_stop
