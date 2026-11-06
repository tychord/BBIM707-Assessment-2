import csv
import json
import time
from kafka import KafkaProducer

csv_path = "/home/hp/BBIM707_Task3/olist_integrated_clean.csv"
topic = "olist_orders_stream"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5
)

sent = 0

with open(csv_path, newline="", encoding="utf-8") as file:
    reader = csv.DictReader(file)

    for row in reader:
        if sent >= 100:
            break

        event = {
            "order_id": row["order_id"],
            "order_status": row["order_status"],
            "delivery_days": row["delivery_days"],
            "delivery_delay_days": row["delivery_delay_days"]
        }

        future = producer.send(
            topic,
            key=row["order_id"],
            value=event
        )

        metadata = future.get(timeout=30)
        sent += 1

        print(
            f"Event {sent}: "
            f"Partition={metadata.partition}, "
            f"Offset={metadata.offset}"
        )

        time.sleep(0.1)

producer.flush()
producer.close()

print("Total Olist Events Published:", sent)
