from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from xml_to_neo import parse_xml_file

with DAG(dag_id='xml_to_neo',
         description='xml_to_neo',
         schedule_interval=None,
         start_date=datetime(2021, 1, 1),
         catchup=False,
         tags=['xml_to_neo']) as xml_to_neo:
    start = EmptyOperator(task_id='start')

    extract_and_store = PythonOperator(task_id='extract_and_store',
                                       python_callable=parse_xml_file,
                                       op_kwargs={"xml_file_path": '/opt/airflow//plugins/data/Q9Y261.xml',
                                                  "neo4j_uri":     'neo4j://myneo:7687',
                                                  "username":      'neo4j',
                                                  "password":      'password'})

    end = EmptyOperator(task_id='end')

    start >> extract_and_store >> end
