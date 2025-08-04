



# import os
# import sys
# import re

# import pandas_gbq
# from airflow import DAG
# from datetime import datetime
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator, ShortCircuitOperator
# from airflow.providers.slack.operators.slack import SlackAPIPostOperator
# from kubernetes import client as k8s


# # def get_n_rows_yesterday(**context):
# #     project = 'teamsykefravr-prod-7e29'
# #     sql = 'SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT utsendt_forsok_tidspunkt FROM utsending_varsel_feilet where utsendt_forsok_tidspunkt > CURRENT_DATE - 1 and utsendt_forsok_tidspunkt < CURRENT_DATE;")'

# #     df = pandas_gbq.read_gbq(sql, project_id=project)
# #     count = len(df)
# #     context['ti'].xcom_push(key='row_count', value=count)
# #     return count

# def get_n_rows_yesterday(**context):
#     raise Exception("Testfeil - dette skal trigge Slack-varsling!")


# def skal_sendes_slack(**context):
#     count = context['ti'].xcom_pull(task_ids='varsel_status', key='row_count')
#     return count > 0  # Bare kjør videre hvis count > 0

# def send_failure_alert(context):
#     """
#     Sender Slack-melding hvis noen task i DAG feiler.
#     """
#     dag_id = context.get('dag').dag_id
#     task_id = context.get('task_instance').task_id
#     execution_date = context.get('execution_date')
#     log_url = context.get('task_instance').log_url

#     message = (
#         f":red_circle: DAG `{dag_id}` feilet!\n"
#         f"Task: `{task_id}`\n"
#         f"Kjøring: {execution_date}\n"
#         f"Se logg: {log_url}"
#     )

#     slack_alert = SlackAPIPostOperator(
#         task_id="slack_failure_alert",
#         slack_conn_id="slack_connection",  # Sett inn din Slack connection id her
#         channel="#syfortellinger-alert",         # Sett inn Slack-kanalen for feilvarsler
#         text=message,
#     )
#     slack_alert.execute(context=context)


# with DAG(
#     'overvakning',
#     schedule_interval="0 4 * * *",
#     start_date=datetime(2025, 7, 10),
#     catchup=False,
#     on_failure_callback=send_failure_alert 
# ) as dag:

#     t_feilet_status = PythonOperator(
#         task_id='varsel_status',
#         python_callable=get_n_rows_yesterday,
#         provide_context=True,
#     )

#     t_sjekk_send = ShortCircuitOperator(
#         task_id='sjekk_send_slack',
#         python_callable=skal_sendes_slack,
#         provide_context=True,
#     )

#     t_varsling = SlackAPIPostOperator(
#         task_id="varsling",
#         slack_conn_id="slack_connection",
#         channel="#syfortellinger-alert",
#         text="NB! Nye rader i esyfovarsel.utsendt_varsel_feilet i går. Antall rader: {{ ti.xcom_pull(task_ids='varsel_status', key='row_count') }}",
#         executor_config={
#             "pod_override": k8s.V1Pod(
#                 metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
#             )
#         },
#     )

#     t_stop = EmptyOperator(task_id="stop_task")

#     t_feilet_status >> t_sjekk_send >> t_varsling >> t_stop


import os
import sys
import re
import pandas_gbq
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from kubernetes import client as k8s


def get_n_rows_yesterday(**context):
    # Simuler feil for testing av feilmelding i Slack
    raise Exception("Testfeil - dette skal trigge Slack-varsling!")


def skal_sendes_slack(**context):
    count = context['ti'].xcom_pull(task_ids='varsel_status', key='row_count')
    return count > 0  # Bare kjør videre hvis count > 0


def send_failure_alert(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('dag').dag_id

    message = f":x: Feil i task `{task_id}` i DAG `{dag_id}`"

    hook = SlackWebhookHook(
        http_conn_id='slack_connection',  # Din Slack webhook-connection i Airflow
        message=message,
        channel="#syfortellinger-alert",
    )
    hook.execute()


with DAG(
    'overvakning',
    schedule_interval="0 4 * * *",
    start_date=datetime(2025, 7, 10),
    catchup=False,
    on_failure_callback=send_failure_alert  # Koble på feilhåndtering her
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

    t_varsling = PythonOperator(
        task_id='varsling',
        python_callable=lambda **context: SlackWebhookHook(
            http_conn_id='slack_connection',
            message="NB! Nye rader i esyfovarsel.utsendt_varsel_feilet i går. Antall rader: "
                    f"{context['ti'].xcom_pull(task_ids='varsel_status', key='row_count')}",
            channel="#syfortellinger-alert"
        ).execute(),
    )

    t_stop = EmptyOperator(task_id="stop_task")

    t_feilet_status >> t_sjekk_send >> t_varsling >> t_stop
