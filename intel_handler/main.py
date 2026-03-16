from kafka_consumer import KafkaConsumer

consumer = KafkaConsumer("localhost:9092", "intel", "intel_handler")

consumer.consume()