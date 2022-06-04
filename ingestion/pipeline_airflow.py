from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dependencies import prepare, get_summoners, get_summoners_puuid, get_matches_id, get_match_data


dag = DAG('lol_de', description='Lol Ranked Games', 
            schedule_interval=timedelta(days=1),
            start_date=datetime.today() - timedelta(days=1), catchup=False)

prepare_operator = PythonOperator(task_id='prepare', python_callable=prepare.prepare, dag=dag)
get_summoners_operator = PythonOperator(task_id='get_summoners', python_callable=get_summoners.get_summoners, dag=dag)
get_summoners_puuid_operator = PythonOperator(task_id='get_summoners_puuid', python_callable=get_summoners_puuid.get_summoners_puuid, dag=dag)
get_matches_id_operator = PythonOperator(task_id='get_matches_id', python_callable=get_matches_id.get_matches_id, dag=dag)
get_match_data_operator = PythonOperator(task_id='get_match_data', python_callable=get_match_data.get_match_data, dag=dag)

prepare_operator >> get_summoners_operator >> get_summoners_puuid_operator >> get_matches_id_operator >> get_match_data_operator
