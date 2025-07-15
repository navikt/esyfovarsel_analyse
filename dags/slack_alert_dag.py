



import os
import sys
import re

import pandas_gbq
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from kubernetes import client as k8s


def get_n_rows_yesterday(**context):
    # project = 'teamsykefravr-prod-7e29'
    # sql = '''
    # SELECT * FROM EXTERNAL_QUERY(
    #     "team-esyfo-prod-bbe6.europe-north1.esyfovarsel",
    #     "SELECT utsendt_forsok_tidspunkt FROM utsending_varsel_feilet 
    #      WHERE utsendt_forsok_tidspunkt > CURRENT_DATE - 1 
    #      AND utsendt_forsok_tidspunkt < CURRENT_DATE;"
    # )
    # '''
    # df = pandas_gbq.read_gbq(sql, project_id=project)
    count = 2 #len(df)
    context['ti'].xcom_push(key='row_count', value=count)
    return count


def skal_sendes_slack(**context):
    count = context['ti'].xcom_pull(task_ids='varsel_status', key='row_count')
    return count > 0  # Bare kjør videre hvis count > 0


with DAG(
    'overvakning',
    schedule_interval="0 4 * * *",
    start_date=datetime(2025, 7, 10),
    catchup=False
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
        channel="#syfortellinger-alert",
        text="NB!Test Nye rader i esyfovarsel.utsendt_varsel_feilet i går. Antall rader: {{ ti.xcom_pull(task_ids='varsel_status', key='row_count') }}",
        executor_config={
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
            )
        },
    )

    t_stop = EmptyOperator(task_id="stop_task")

    t_feilet_status >> t_sjekk_send >> t_varsling >> t_stop


'''
import os
import sys
import re
from airflow import DAG
import datetime as dt
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

from kubernetes import client as k8s


def get_n_rows_yesterday():

    import pandas_gbq
 
    project = 'teamsykefravr-prod-7e29'

    sql = 'SELECT * FROM EXTERNAL_QUERY("team-esyfo-prod-bbe6.europe-north1.esyfovarsel", "SELECT utsendt_forsok_tidspunkt FROM utsending_varsel_feilet where utsendt_forsok_tidspunkt > CURRENT_DATE - 1 and utsendt_forsok_tidspunkt < CURRENT_DATE;")'

    df = pandas_gbq.read_gbq(sql, project_id=project)

    return len(df)


def varsel_status():
    
    len_yesterday = get_n_rows_yesterday()

    if len_yesterday > 0:
        ret= 'varsling'
    else:
        ret='stop_task'

    return ret



with DAG('overvakning',
         schedule_interval = "0 4 * * *",
         start_date = datetime(2025, 7, 10)
        ) as dag:
   
  
    t_feilet_status = PythonOperator(
        task_id='varsel_status', 
        python_callable=varsel_status,
        dag=dag
    )
 
    t_varsling = SlackAPIPostOperator(
        task_id="varsling",
        slack_conn_id="slack_connection",
        text=f"NB! Nye rader i esyfovarsel.utsendt_varsel_feilet i går. Ikke vellykket resending. Antall rader: {get_n_rows_yesterday()}",
        channel="#syfortellinger-alert",
        executor_config={
              "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(annotations={"allowlist": "slack.com"})
              )
            },

    )

    t_stop = EmptyOperator(task_id="stop_task", dag=dag)

t_feilet_status >> t_stop >>  t_varsling 
'''