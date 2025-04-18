import json
import time
# import subprocess
from kafka import KafkaProducer

TOPIC_NAME = "yelp_reviews"
KAFKA_BROKER = "localhost:9092"
DATA_PATH = "/home/sriganesh/Downloads/yelp_academic_dataset_review.json"  # 🔁 Replace with actual path to Yelp JSON file

# # Create topic (1 partition, 1 replica)
# subprocess.call([
#     "~/kafka_2.13-4.0.0/bin/kafka-topics.sh",
#     "--create",
#     "--if-not-exists",
#     "--topic", TOPIC_NAME,
#     "--bootstrap-server", KAFKA_BROKER,
#     "--partitions", "1",
#     "--replication-factor", "1"
# ])

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def stream_json_data(filepath, max_records=200_000):
    count = 0
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if count >= max_records:
                print(f"✅ Sent 2 thousand records.")
                break
            try:
                entry = json.loads(line)
                review_data = {
                    "text": entry["text"],
                    "stars": entry["stars"]
                }
                producer.send(TOPIC_NAME, value=review_data)
                count += 1

                # Every 1000 records, print and delay
                if count % 1000 == 0:
                    print(f"📤 Sent {count} records...")
                    time.sleep(0.2)

            except Exception as e:
                print(f"⚠️ Skipped malformed line: {e}")
    producer.flush()

if __name__ == "__main__":
    stream_json_data(DATA_PATH)
