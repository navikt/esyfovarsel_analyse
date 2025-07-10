
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

t_feilet_status >>  t_varsling 
