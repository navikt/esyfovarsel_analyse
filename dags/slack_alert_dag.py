
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

    if len(re.findall('/', os.getcwd())) == 5:
        base_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir, os.pardir))
    else: 
        base_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

    sys.path.append(base_path)

    from tools import get_dict 
    import pandas_gbq
 
    project = 'teamsykefravr-prod-7e29'
    d_sql = get_dict(base_path + "/esyfovarsel.sql")

    df = pandas_gbq.read_gbq(d_sql['esyfovarsel_alt'], project_id=project)

    return len(df)

def varsel_status():
    
    len_yesterday = get_n_rows_yesterday()

    if len_yesterday > 0:
        ret= 'varsling'
    else:
        ret='stop_task'
    return ret



with DAG('overvakning', 
         start_date= datetime(2025, 7, 10),
         end_date= datetime(2025, 7, 12)
        ) as dag:
   
  
    t_feilet_status = PythonOperator(
        task_id='varsel_status', 
        python_callable=varsel_status,
        dag=dag
    )
 
    t_varsling = SlackAPIPostOperator(
        task_id="varsling",
        slack_conn_id="slack_connection",
        text=f"NB! Nye rader i esyfovarsel.utsendt_varsel_feilet i går. Ikke vellykket resending.",
        channel="#syfortellinger-alert"

    )

    t_stop = EmptyOperator(task_id='stop_task', dag=dag)


t_feilet_status >>  t_varsling >> t_stop 
