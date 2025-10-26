import json, time, uuid, requests, os, random, datetime as dt
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "rides.source")
URL = "https://raw.githubusercontent.com/idiattara/Spark_DIATTARA/main/data_projet.json"

p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
data = requests.get(URL, timeout=30).json()

def send(ev):
    ev = dict(ev)
    ev["event_id"] = str(uuid.uuid4())
    ev["ts"] = dt.datetime.utcnow().isoformat() + "Z"
    p.produce(TOPIC, json.dumps(ev).encode("utf-8"))
    p.poll(0)

if __name__ == "__main__":
    i = 0
    while True:
        send(random.choice(data))
        i += 1
        if i % 100 == 0:
            print(f"sent {i} events")
        time.sleep(0.05)
