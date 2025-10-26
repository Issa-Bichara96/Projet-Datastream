from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Consumer, Producer
import json, math, os, time

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_IN, TOPIC_OUT = "rides.source", "rides.result"

def haversine(lat1, lon1, lat2, lon2):
    R=6371.009
    from math import radians, sin, cos, asin, sqrt
    dphi=radians(lat2-lat1); dl=radians(lon2-lon1)
    a=sin(dphi/2)**2+cos(radians(lat1))*cos(radians(lat2))*sin(dl/2)**2
    return 2*R*asin(sqrt(a))

def compute_and_publish(**_):
    c = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP,"group.id":"airflow-compute",
                  "auto.offset.reset":"earliest","enable.auto.commit":False})
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    c.subscribe([TOPIC_IN])
    comfort_mult={"low":1.0,"medium":1.2,"high":1.5}; km_price=1.2
    start=time.time()
    while time.time()-start<25:
        msg=c.poll(1.0)
        if not msg or msg.error(): continue
        ev=json.loads(msg.value().decode("utf-8"))
        dist=haversine(ev["client_lat"],ev["client_lon"],ev["driver_lat"],ev["driver_lon"])
        mult=comfort_mult.get(ev.get("comfort","low"),1.0)
        price=round(ev.get("base_price",2.0)+dist*km_price*mult,2)
        out={**ev,"distance_km":round(dist,3),"price_eur":price,
             "compute_ts":datetime.utcnow().isoformat()+"Z"}
        p.produce(TOPIC_OUT, json.dumps(out).encode("utf-8"))
    p.flush(5); c.close()

default_args={"owner":"data","retries":1,"retry_delay":timedelta(minutes=1)}
with DAG("dag1_compute_cost_travel", start_date=datetime(2025,10,1),
         schedule_interval="* * * * *", catchup=False, default_args=default_args,
         max_active_runs=1, description="Compute distance & price, publish to result"):
    PythonOperator(task_id="consume_compute_publish", python_callable=compute_and_publish)
