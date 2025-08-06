import os
import sys
import re
import pandas_gbq
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.trigger_rule import TriggerRule
from kubernetes import client as k8s


def get_esyfo_n_rows_yesterday():
    project = 'teamsykefravr-prod-7e29'
    sql = """
SELECT * FROM EXTERNAL_QUERY(
    "team-esyfo-prod-bbe6.europe-north1.esyfovarsel",
    "SELECT utsendt_tidspunkt, type, kanal FROM utsendt_varsel WHERE utsendt_tidspunkt > CURRENT_DATE - 101 AND utsendt_tidspunkt < CURRENT_DATE AND type = 'SM_DIALOGMOTE_INNKALT' AND kanal = 'DITT_SYKEFRAVAER';"
)
"""
    df = pandas_gbq.read_gbq(sql, project_id=project)
    return len(df)


def get_dialogmote_n_rows_yesterday():
    project = 'teamsykefravr-prod-7e29'
    sql = """
SELECT * FROM EXTERNAL_QUERY(
    "teamsykefravr-prod-7e29.europe-north1.dialogmote",
    "SELECT cast(id as text), cast(uuid as text), created_at, updated_at, cast(mote_id as text), status, opprettet_av, tilfelle_start, published_at, motedeltaker_behandler FROM mote_status_endret WHERE created_at > CURRENT_DATE - 101 AND created_at < CURRENT_DATE AND status = 'INNKALT';"
)
"""
    df = pandas_gbq.read_gbq(sql, project_id=project)
    return len(df)


def varsel_status(**context):
    # len_esyfo = get_esyfo_n_rows_yesterday()
    # len_dialogmote = get_dialogmote_n_rows_yesterday()
    # diff = len_dialogmote - len_esyfo

    # status = 'varsling' if diff > 0 else 'stop_task'

    # context['ti'].xcom_push(key='diff', value=diff)
    # context['ti'].xcom_push(key='status', value=status)
    # return status
    raise Exception("Simulated failure for testing purposes.")


def er_varsling(**context):
    status = context['ti'].xcom_pull(task_ids='varsel_status', key='status')
    return status == 'varsling'


with DAG(
    dag_id='overvakning_test_diff',
    schedule_interval="0 4 * * *",
    start_date=datetime(2025, 7, 10),
    catchup=False,
) as dag:

    # Task 1: Hent status og differanse
    t_varsel_status = PythonOperator(
        task_id='varsel_status',
        python_callable=varsel_status,
        provide_context=True,
    )

    # Task 2: Sjekk om varsling skal skje
    t_sjekk_om_varsling = ShortCircuitOperator(
        task_id='sjekk_om_varsling',
        python_callable=er_varsling,
        provide_context=True,
    )

    # Task 3: Slack-varsel ved datadifferanse
    t_send_slack = SlackAPIPostOperator(
        task_id='send_slack',
        slack_conn_id='slack_connection',
        channel='#esyfo-data-alert',
        text=(
            "NB!test varsel Data differanse mellom "
            "`esyfo-utsendt_varsel-SM_DIALOGMOTE_INNKALT` og "
            "`isyfo-dialogmote-INNKALT` i går.\n"
            "Antall differanse: {{ ti.xcom_pull(task_ids='varsel_status', key='diff') }}."
        ),
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
            )
        },
    )

    # Task 4: Slack-varsel ved feil
    slack_ved_feil = SlackAPIPostOperator(
        task_id='slack_ved_feil',
        slack_conn_id='slack_connection',
        channel='#syfortellinger-alert',
        text=":error: test varsel Feil i DAG `overvakning_test_diff`!* Se Airflow for detaljer.",
        trigger_rule=TriggerRule.ONE_FAILED,  # Kjør denne hvis en eller flere upstream-tasks feiler
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
            )
        },
    )

    # Task 5: Dummy slutt
    t_stop = EmptyOperator(task_id='stop_task')

    # Sett opp flyt
    t_varsel_status >> [t_sjekk_om_varsling, slack_ved_feil]
    t_sjekk_om_varsling >> t_send_slack >> t_stop

    # Sørg for at slutt og feil-varsling er koblet riktig
    for task in [t_sjekk_om_varsling, t_send_slack, t_stop]:
        task >> slack_ved_feil
