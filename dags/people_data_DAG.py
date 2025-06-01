from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
import os
import sys


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../plugins')))


from people_kafka_producer import fetch_and_produce
from sending_to_s3 import start_sending_to_s3
from sending_to_postgre import processing_data
from sending_to_mongo import processing_data_mongo



logger = logging.getLogger('dag_logger')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025,4,14)
}

dag = DAG(
    dag_id="random_name_api_dag",
    default_args=default_args,
    "catchup"= False,
    schedule_interval="*/5 * * * *",
    max_active_runs=1
)

def start_job():
    logging.info("Starting the people data processing pipeline.")


def producing_data():
    try:
        logger.info("Producing the Data...")
        fetch_and_produce()
    
    except Exception as e:
        logger.error(f"An error occurred while producing people data: {e}")
        raise


def sending_data_to_s3():
    logger.info("Sending raw data into s3.")
    start_sending_to_s3()

def sending_data_to_postgres():
    logger.info("Sending people data into postgresql.")
    processing_data()

def sending_data_to_mongo():
    logger.info("Sending people data into mongo.")
    processing_data_mongo()

def end_data_job():
    logger.info("Data processing pipeline finished.")


start_task = PythonOperator(
    task_id='start_job',
    python_callable=start_job,
    dag=dag
)

producing_data_task = PythonOperator(
    task_id='producing_data_job',
    python_callable=producing_data,
    dag=dag
)

sending_data_to_s3_task = PythonOperator(
    task_id='sending_data_to_s3_job',
    python_callable=sending_data_to_s3,
    dag=dag
)

sending_data_to_postgres_task = PythonOperator(
    task_id='sending_data_to_postgres_job',
    python_callable=sending_data_to_postgres,
    dag=dag
)

sending_data_to_mongo_task = PythonOperator(
    task_id='sending_data_to_mongo_job',
    python_callable=sending_data_to_mongo,
    dag=dag
)


end_task = PythonOperator(
    task_id='end_data_job',
    python_callable=end_data_job,
    dag=dag
)

start_task >> producing_data_task >> sending_data_to_s3_task >> sending_data_to_postgres_task >> sending_data_to_mongo_task >> end_task
