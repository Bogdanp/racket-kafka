from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    compression_type=None,
    linger_ms=10000,
)
for i in range(1_000_000):
    producer.send("bench-publish-1M", key=b"k", value=b"v", partition=i % 8)
producer.flush()
