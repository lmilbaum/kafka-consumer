#!/usr/bin/env python3

import confluent_kafka
import dotenv
import os


def process_message(msg):
    data = msg.value().decode("utf-8")
    print("Processing message:", data)


def main():
    dotenv.load_dotenv()

    conf = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "group.id": os.getenv("GROUP_ID"),
        "auto.offset.reset": os.getenv("AUTO_OFFSET_RESET"),
        "security.protocol": os.getenv("SECURITY_PROTOCOL"),
        "sasl.mechanisms": os.getenv("SASL_MECHANISMS"),
        "sasl.username": os.getenv("SASL_USERNAME"),
        "sasl.password": os.getenv("SASL_PASSWORD"),
    }

    consumer = confluent_kafka.Consumer(conf)

    topic = os.getenv("KAFKA_TOPIC")

    try:
        consumer.subscribe([topic])
    except Exception as e:
        print(e)

    print(f"Subscribed to topic: {topic}")
    print("Waiting for messages... Press Ctrl+C to exit.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise confluent_kafka.KafkaException(msg.error())

            process_message(msg)

    except KeyboardInterrupt:
        print("\nExiting consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
