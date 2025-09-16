import sys
import six
import dotenv
import os

if sys.version_info >= (3, 12, 0):
    sys.modules["kafka.vendor.six.moves"] = six.moves
from kafka import KafkaConsumer

# These would come from env variables and/or secrets.
kafka_hosts = [
    "b-3.itsandbox.642qp2.c5.kafka.us-east-1.amazonaws.com:9096",
    "b-1.itsandbox.642qp2.c5.kafka.us-east-1.amazonaws.com:9096",
    "b-2.itsandbox.642qp2.c5.kafka.us-east-1.amazonaws.com:9096",
]
topic = ""
user_name = ""
password = ""


def main():
    print("Connecting to Kafka...")

    dotenv.load_dotenv()

    # generating the Kafka Consumer
    my_consumer = KafkaConsumer(
        os.getenv("KAFKA_TOPIC"),
        bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"),
        auto_offset_reset="earliest",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=os.getenv("SASL_USERNAME"),
        sasl_plain_password=os.getenv("SASL_PASSWORD"),
        enable_auto_commit=True,
        api_version=(3, 7, 0),
    )

    print("Connected!")

    for message in my_consumer:
        print(str(message.value, encoding="utf-8"))

    my_consumer.close()


if __name__ == "__main__":
    main()
