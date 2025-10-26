from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Consumer
from elasticsearch import Elasticsearch
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "rides.result"
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ES_INDEX = "rides"

def check_kafka(**_):
    c = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "monitor", "auto.offset.reset": "latest"})
    c.subscribe([TOPIC])
    msg = c.poll(1.0)
    if msg:
        print("Kafka OK: message reçu")
    else:
        print("Kafka WARNING: aucun message reçu")
    c.close()

def check_es(**_):
    es = Elasticsearch(ES_URL)
    count = es.count(index=ES_INDEX)["count"]
    print(f"ES OK: {count} documents dans l'index {ES_INDEX}")

default_args = {"owner": "data", "retries": 1, "retry_delay": timedelta(minutes=1)}

with DAG("dag3_monitor_pipeline", start_date=datetime(2025,10,1),
         schedule_interval="*/5 * * * *", catchup=False, default_args=default_args,
         description="Surveille Kafka et Elasticsearch") as dag:

    t1 = PythonOperator(task_id="check_kafka", python_callable=check_kafka)
    t2 = PythonOperator(task_id="check_es", python_callable=check_es)
