



import os
import sys
import re

# import pandas_gbq
# from airflow import DAG
# from datetime import datetime
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator, ShortCircuitOperator
# from airflow.providers.slack.operators.slack import SlackAPIPostOperator
# from kubernetes import client as k8s


# def get_n_rows_yesterday(**context):
    
#     #raise Exception("Testfeil! Dette er med vilje for å teste on_failure_callback")

#     project = 'teamsykefravr-prod-7e29'
#     sql = 'SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT utsendt_forsok_tidspunkt FROM utsending_varsel_feilet where utsendt_forsok_tidspunkt > CURRENT_DATE - 1 and utsendt_forsok_tidspunkt < CURRENT_DATE;")'

#     df = pandas_gbq.read_gbq(sql, project_id=project)
#     count = len(df)
#     context['ti'].xcom_push(key='row_count', value=count)
#     return count


# def skal_sendes_slack(**context):
#     count = context['ti'].xcom_pull(task_ids='varsel_status', key='row_count')
#     return count > 0  # Bare kjør videre hvis count > 0

# # def slack_failure_alert(context):
# #     dag_id = context.get('dag').dag_id
# #     task_id = context.get('task_instance').task_id
# #     execution_date = context.get('ts')
# #     log_url = context.get('task_instance').log_url
# #     exception = context.get('exception')

# #     message = (
# #         f":rotating_light: *Airflow DAG Failure Alert!*\n\n"
# #         f"*DAG*: `{dag_id}`\n"
# #         f"*Task*: `{task_id}`\n"
# #         f"*Execution Time*: `{execution_date}`\n"
# #         f"*Exception*: `{exception}`\n"
# #         f"*Log URL*: {log_url}"
# #     )

# #     alert = SlackAPIPostOperator(
# #         task_id='slack_failure_alert',
# #         slack_conn_id='slack_connection',
# #         channel='#syfortellinger-alert',
# #         text=message,
# #     )
# #     return alert.execute(context=context)


# with DAG(
#     'overvakning',
#     schedule_interval="0 4 * * *",
#     start_date=datetime(2025, 7, 10),
#     catchup=False,
#     #on_failure_callback=slack_failure_alert,
#     #default_args={"retries": 1}, 
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
#     sql = '''
#         SELECT * FROM EXTERNAL_QUERY(
#             "team-esyfo-prod-bbe6.europe-north1.esyfovarsel",
#             "SELECT utsendt_forsok_tidspunkt 
#              FROM utsending_varsel_feilet 
#              WHERE utsendt_forsok_tidspunkt > CURRENT_DATE - 1 
#                AND utsendt_forsok_tidspunkt < CURRENT_DATE;"
#         )
#     '''

#     df = pandas_gbq.read_gbq(sql, project_id=project)
#     count = len(df)
#     context['ti'].xcom_push(key='row_count', value=count)
#     return count


# def skal_sendes_slack(**context):
#     count = context['ti'].xcom_pull(task_ids='varsel_status', key='row_count')
#     return count > 0  # Bare kjør videre hvis count > 0


# with DAG(
#     dag_id='overvakning',
#     schedule_interval="0 4 * * *",
#     start_date=datetime(2025, 7, 10),
#     catchup=False,
#     default_args={
#         "retries": 1
#     }
# ) as dag:

#     # Task: Hent antall rader
#     t_feilet_status = PythonOperator(
#         task_id='varsel_status',
#         python_callable=get_n_rows_yesterday,
#         provide_context=True,
#     )

#     # Task: Slack-alert hvis varsel_status feiler
#     slack_ved_feil = SlackAPIPostOperator(
#         task_id='slack_ved_feil_i_feilet_status',
#         slack_conn_id='slack_connection',
#         channel='#syfortellinger-alert',
#         text=":rotating_light: *Task `varsel_status` feilet i DAG `overvakning`!*",
#         trigger_rule=TriggerRule.ONE_FAILED,
#     )

#     # Task: Sjekk om vi skal sende Slack-varsel for rader
#     t_sjekk_send = ShortCircuitOperator(
#         task_id='sjekk_send_slack',
#         python_callable=skal_sendes_slack,
#         provide_context=True,
#     )

#     # Task: Slack-varsling dersom det finnes rader
#     t_varsling = SlackAPIPostOperator(
#         task_id="varsling",
#         slack_conn_id="slack_connection",
#         channel="#syfortellinger-alert",
#         text=(
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

#     # Oppsett av rekkefølge og feilhåndtering
#     t_feilet_status >> [t_sjekk_send, slack_ved_feil]
#     t_sjekk_send >> t_varsling >> t_stop


# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.slack.operators.slack import SlackAPIPostOperator
# from airflow.utils.trigger_rule import TriggerRule
# from datetime import datetime


# def feilende_funksjon(**context):
#     raise Exception("Dette er en test-feil for å utløse Slack-varsling!")


# with DAG(
#     dag_id='test_slack_feil',
#     start_date=datetime(2025, 8, 1),
#     schedule_interval=None,  # Manuell kjøring for test
#     catchup=False,
# ) as dag:

#     feilende_task = PythonOperator(
#         task_id='feilende_task',
#         python_callable=feilende_funksjon,
#         provide_context=True,
#     )

#     slack_ved_feil = SlackAPIPostOperator(
#         task_id='slack_ved_feil',
#         slack_conn_id='slack_connection',
#         channel='#syfortellinger-alert',
#         text=":rotating_light: *Task `feilende_task` feilet i DAG `test_slack_feil`!*",
#         trigger_rule=TriggerRule.ONE_FAILED,  # Kun hvis `feilende_task` feiler
#     )

#     feilende_task >> slack_ved_feil


# from airflow import DAG
# from airflow.operators.python import PythonOperator, ShortCircuitOperator
# from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
# from airflow.utils.trigger_rule import TriggerRule
# from datetime import datetime

# def feilende_funksjon(**context):
#     raise Exception("💥 Test-feil i feilende_task!")

# def sjekk_dummy(**context):
#     return True

# with DAG(
#     dag_id='test_dag_slack_webhook_feil',
#     start_date=datetime(2025, 8, 1),
#     schedule_interval=None,
#     catchup=False,
#     default_args={"retries": 0},
# ) as dag:

#     feilende_task = PythonOperator(
#         task_id='feilende_task',
#         python_callable=feilende_funksjon,
#     )

#     slack_ved_feil = SlackWebhookOperator(
#         task_id='slack_ved_feil',
#         slack_webhook_conn_id='slack_connection',  # <-- rett parameter her
        
#         message=":rotating_light: *Task `feilende_task` feilet i DAG `test_dag_slack_webhook_feil`!*",
#         trigger_rule=TriggerRule.ONE_FAILED,
#     )

#     t_sjekk_send = ShortCircuitOperator(
#         task_id='t_sjekk_send',
#         python_callable=sjekk_dummy,
#     )

#     feilende_task >> [slack_ved_feil, t_sjekk_send]


# import os
# from airflow import DAG
# from airflow.operators.python import PythonOperator, ShortCircuitOperator
# from airflow.providers.slack.operators.slack import SlackAPIPostOperator
# from airflow.utils.trigger_rule import TriggerRule
# from datetime import datetime
# from kubernetes import client as k8s

# def feilende_funksjon(**context):
#     raise Exception("💥 Test-feil i feilende_task!")

# def sjekk_dummy(**context):
#     return True

# with DAG(
#     dag_id='test_dag_slack_api_feil',
#     start_date=datetime(2025, 8, 1),
#     schedule_interval=None,
#     catchup=False,
#     default_args={"retries": 0},
# ) as dag:

#     feilende_task = PythonOperator(
#         task_id='feilende_task',
#         python_callable=feilende_funksjon,
#     )

#     slack_ved_feil = SlackAPIPostOperator(
#         task_id='slack_ved_feil',
#         slack_conn_id='slack_connection',  # Airflow connection med Slack bot token
#         channel="#syfortellinger-alert",      # Sett kanal her
#         text=":rotating_light: *Task `feilende_task` feilet i DAG `test_dag_slack_api_feil`!*",
#         trigger_rule=TriggerRule.ONE_FAILED,  # Kjør om en foregående task feiler
#         executor_config={
#             "pod_override": k8s.V1Pod(
#                 metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
#             )
#         },
#     )

#     t_sjekk_send = ShortCircuitOperator(
#         task_id='t_sjekk_send',
#         python_callable=sjekk_dummy,
#     )

#     # Hvis feil i feilende_task, send slack alert, ellers kjør sjekk
#     feilende_task >> [slack_ved_feil, t_sjekk_send]



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
    sql = 'SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT utsendt_forsok_tidspunkt FROM utsending_varsel_feilet where utsendt_forsok_tidspunkt > CURRENT_DATE - 7 and utsendt_forsok_tidspunkt < CURRENT_DATE;")'

    
    df = pandas_gbq.read_gbq(sql, project_id=project)
    count = len(df)
    context['ti'].xcom_push(key='row_count', value=count)
    return count


def skal_sendes_slack(**context):
    count = context['ti'].xcom_pull(task_ids='varsel_status', key='row_count')
    return count > 0  # Kjør videre kun hvis count > 0


with DAG(
    dag_id='overvakning',
    schedule_interval="0 4 * * *",
    start_date=datetime(2025, 7, 10),
    catchup=False,
    default_args={"retries": 1},
) as dag:

    # Hent antall rader
    t_feilet_status = PythonOperator(
        task_id='varsel_status',
        python_callable=get_n_rows_yesterday,
        provide_context=True,
    )

    # Slack-varsling hvis varsel_status task feiler
    slack_ved_feil = SlackAPIPostOperator(
        task_id='slack_ved_feil_i_feilet_status',
        slack_conn_id='slack_connection',
        channel='#syfortellinger-alert',
        text=":rotating_light:  *testvarsle:Task `varsel_status` feilet i DAG `overvakning`!*",
        trigger_rule=TriggerRule.ONE_FAILED,  # Kjør om denne eller noen forrige feiler
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
            )
        },
    )

    # Sjekk om det finnes rader og skal varsle videre
    t_sjekk_send = ShortCircuitOperator(
        task_id='sjekk_send_slack',
        python_callable=skal_sendes_slack,
        provide_context=True,
    )

    # Slack-varsling dersom rader funnet
    t_varsling = SlackAPIPostOperator(
        task_id="varsling",
        slack_conn_id="slack_connection",
        channel="#syfortellinger-alert",
        text=(
            "testvarsle. "
            "NB! Nye rader i `esyfovarsel.utsendt_varsel_feilet` i går. "
            "Antall rader: {{ ti.xcom_pull(task_ids='varsel_status', key='row_count') }}"
        ),
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
            )
        },
    )

    t_stop = EmptyOperator(task_id="stop_task")

    # Rekkefølge:
    # Når varsel_status kjører, så på feilmelding send slack_ved_feil,
    # ellers sjekk om varsling skal sendes for rader
    t_feilet_status >> [t_sjekk_send, slack_ved_feil]
    t_sjekk_send >> t_varsling >> t_stop
