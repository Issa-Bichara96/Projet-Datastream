from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Consumer
from elasticsearch import Elasticsearch
from google.cloud import storage
import json, os, time
from io import BytesIO

KAFKA_BOOTSTRAP=os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
TOPIC="rides.result"; ES_URL=os.getenv("ES_URL","http://elasticsearch:9200")
ES_INDEX="rides"; GCS_BUCKET=os.getenv("GCS_BUCKET","your-bucket")
GCS_PREFIX="rides/ingested/date="

def consume_transform_put(**_):
    c=Consumer({"bootstrap.servers":KAFKA_BOOTSTRAP,"group.id":"airflow-transform",
                "auto.offset.reset":"earliest","enable.auto.commit":False})
    c.subscribe([TOPIC])
    es=Elasticsearch(ES_URL)
    # Pour GCS, la variable GOOGLE_APPLICATION_CREDENTIALS doit pointer vers un JSON de service.
    gcs=storage.Client(); bucket=gcs.bucket(GCS_BUCKET)

    batch=[]; start=time.time(); today=datetime.utcnow().strftime("%Y-%m-%d")
    while time.time()-start<25:
        msg=c.poll(1.0)
        if not msg or msg.error(): continue
        ev=json.loads(msg.value().decode("utf-8"))
        flat={
          "event_id":ev["event_id"], "ts":ev["ts"],
          "agent_timestamp":datetime.utcnow().isoformat()+"Z",
          "client_id":ev.get("client_id"), "driver_id":ev.get("driver_id"),
          "comfort":ev.get("comfort","low"),
          "client_lat":ev["client_lat"], "client_lon":ev["client_lon"],
          "driver_lat":ev["driver_lat"], "driver_lon":ev["driver_lon"],
          "distance_km":ev["distance_km"], "price_eur":ev["price_eur"]
        }
        es.index(index=ES_INDEX, id=flat["event_id"], document=flat)
        batch.append(flat)

    if batch:
        from io import BytesIO
        blob=bucket.blob(f"{GCS_PREFIX}{today}/batch_{int(time.time())}.jsonl")
        payload="\n".join(json.dumps(r) for r in batch).encode("utf-8")
        blob.upload_from_file(BytesIO(payload), content_type="application/json")
    c.close()

default_args={"owner":"data","retries":1,"retry_delay":timedelta(minutes=1)}
with DAG("dag2_transform_index_gcs", start_date=datetime(2025,10,1),
         schedule_interval="* * * * *", catchup=False, default_args=default_args,
         description="Flatten JSON, add agent_timestamp, send to ES + GCS"):
    PythonOperator(task_id="consume_transform_put", python_callable=consume_transform_put)
