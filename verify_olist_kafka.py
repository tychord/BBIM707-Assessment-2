import csv
import json
from kafka import KafkaConsumer

csv_path = "/home/hp/BBIM707_Task3/olist_integrated_clean.csv"

# Load the original 100 Olist records
with open(csv_path, newline="", encoding="utf-8") as f:
    original = list(csv.DictReader(f))[:100]

expected = {
    row["order_id"]: {
        "order_id": row["order_id"],
        "order_status": row["order_status"],
        "delivery_days": row["delivery_days"],
        "delivery_delay_days": row["delivery_delay_days"]
    }
    for row in original
}

# Read messages from Kafka
consumer = KafkaConsumer(
    "olist_orders_stream",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="olist_verification_group",
    consumer_timeout_ms=10000,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

received = []

for message in consumer:
    received.append(message.value)

consumer.close()

# Verify message consistency
received_ids = [event["order_id"] for event in received]

matched = sum(
    1 for event in received
    if event["order_id"] in expected
    and event == expected[event["order_id"]]
)

print("Kafka Message Consistency Verification")
print("Original Records:", len(original))
print("Kafka Messages Received:", len(received))
print("Matching Records:", matched)
print("Duplicate Order IDs:", len(received_ids) - len(set(received_ids)))
print("All Records Match:", matched == len(original) == len(received))
