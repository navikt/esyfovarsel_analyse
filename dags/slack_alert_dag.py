
# import sys
# import re
# import os
# import pandas_gbq
# from airflow import DAG
# from datetime import datetime
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator, ShortCircuitOperator
# from airflow.providers.slack.operators.slack import SlackAPIPostOperator
# from airflow.utils.trigger_rule import TriggerRule
# from kubernetes import client as k8s


# def get_n_rows_yesterday(**context):
#     project = 'teamsykefravr-prod-7e29'
#     sql = 'SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT utsendt_forsok_tidspunkt FROM utsending_varsel_feilet where utsendt_forsok_tidspunkt > CURRENT_DATE - 7 and utsendt_forsok_tidspunkt < CURRENT_DATE;")'

    
#     df = pandas_gbq.read_gbq(sql, project_id=project)
#     count = len(df)
#     context['ti'].xcom_push(key='row_count', value=count)
#     return count


# def skal_sendes_slack(**context):
#     count = context['ti'].xcom_pull(task_ids='varsel_status', key='row_count')
#     return count > 0  # Kjør videre kun hvis count > 0


# with DAG(
#     dag_id='overvakning',
#     schedule_interval="0 4 * * *",
#     start_date=datetime(2025, 7, 10),
#     catchup=False,
#     default_args={"retries": 1},
# ) as dag:

#     # Hent antall rader
#     t_feilet_status = PythonOperator(
#         task_id='varsel_status',
#         python_callable=get_n_rows_yesterday,
#         provide_context=True,
#     )

#     # Slack-varsling hvis varsel_status task feiler
#     slack_ved_feil = SlackAPIPostOperator(
#         task_id='slack_ved_feil_i_feilet_status',
#         slack_conn_id='slack_connection',
#         channel='#syfortellinger-alert',
#         text=":rotating_light:  *testvarsle:Task `varsel_status` feilet i DAG `overvakning`!*",
#         trigger_rule=TriggerRule.ONE_FAILED,  # Kjør om denne eller noen forrige feiler
#         executor_config={
#             "pod_override": k8s.V1Pod(
#                 metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
#             )
#         },
#     )

#     # Sjekk om det finnes rader og skal varsle videre
#     t_sjekk_send = ShortCircuitOperator(
#         task_id='sjekk_send_slack',
#         python_callable=skal_sendes_slack,
#         provide_context=True,
#     )

#     # Slack-varsling dersom rader funnet
#     t_varsling = SlackAPIPostOperator(
#         task_id="varsling",
#         slack_conn_id="slack_connection",
#         channel="#syfortellinger-alert",
#         text=(
#             "testvarsle. "
#             "NB! Nye rader i `esyfovarsel.utsendt_varsel_feilet` i går. "
#             "Antall rader: {{ ti.xcom_pull(task_ids='varsel_status', key='row_count') }}"
#         ),
#         executor_config={
#             "pod_override": k8s.V1Pod(
#                 metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
#             )
#         },
#     )

#     t_stop = EmptyOperator(task_id="stop_task")

#     # Rekkefølge:
#     # Når varsel_status kjører, så på feilmelding send slack_ved_feil,
#     # ellers sjekk om varsling skal sendes for rader
#     t_feilet_status >> [t_sjekk_send, slack_ved_feil]
#     t_sjekk_send >> t_varsling >> t_stop


import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.trigger_rule import TriggerRule

def feile_task(**context):
    raise Exception("💥 Dette er en test-feil!")

def dummy_true(**context):
    return True

def slack_alert_on_failure(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['ts']
    log_url = context['task_instance'].log_url
    exception = context.get('exception')

    message = (
        f":rotating_light: *Task `{task_id}` feilet i DAG `{dag_id}`!* \n"
        f"Execution date: `{execution_date}`\n"
        f"Exception: `{exception}`\n"
        f"Log: {log_url}"
    )

    slack_alert = SlackAPIPostOperator(
        task_id='slack_failure_alert',
        slack_conn_id='slack_connection',
        channel='#syfortellinger-alert',
        text=message,
    )
    return slack_alert.execute(context=context)

with DAG(
    dag_id='test_slack_failure_alert',
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"retries": 0},
    on_failure_callback=slack_alert_on_failure,
) as dag:

    feilende_task = PythonOperator(
        task_id='feilende_task',
        python_callable=feile_task,
    )

    dummy_task = ShortCircuitOperator(
        task_id='dummy_true',
        python_callable=dummy_true,
    )

    feilende_task >> dummy_task
