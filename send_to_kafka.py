from kafka import KafkaProducer, KafkaConsumer
import json
from tqdm import tqdm
import argparse
import os
from enum import Enum
from dotenv import load_dotenv
import time
from kafka.admin import NewTopic, KafkaAdminClient

load_dotenv()


class EnvironmentVariables(str, Enum):
    KAFKA_TOPIC_NAME = 'KAFKA_TOPIC_NAME'
    KAFKA_SERVER = 'KAFKA_SERVER'
    KAFKA_PORT = 'KAFKA_PORT'
    KAFKA_NUM_PARTITIONS = 'KAFKA_NUM_PARTITIONS'
    KAFKA_NUM_REPLICAS = 'KAFKA_NUM_REPLICAS'
    SRC_PARSE = 'SRC_PARSE'

    def get_env(self, variable=None):
        return os.environ.get(self, variable)


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def get_existing_data(topic, servers, port):
    existing_data = []
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[f"{servers}:{port}"],
        auto_offset_reset='earliest',
        consumer_timeout_ms=2000
    )
    for msg in consumer:
        msg_data = json.loads(msg.value)
        existing_data.append(f"{msg_data.get('name')}, {msg_data.get('event_dt')}, {msg_data.get('event_place')}")
    return existing_data


def main():
    topic = EnvironmentVariables.KAFKA_TOPIC_NAME.get_env()
    port = EnvironmentVariables.KAFKA_PORT.get_env()
    servers = EnvironmentVariables.KAFKA_SERVER.get_env()
    num_partitions = EnvironmentVariables.KAFKA_NUM_PARTITIONS.get_env()
    num_replicas = EnvironmentVariables.KAFKA_NUM_REPLICAS.get_env()
    src_name = EnvironmentVariables.SRC_PARSE.get_env()

    if src_name == 'ticketpro':
        from ticketpro import get_all_links_for_parsing, parse_event_by_link, close_driver
        url = 'https://www.ticketpro.by/'
    elif src_name == 'bycard':
        from bycard import get_all_links_for_parsing, parse_event_by_link, close_driver
        url = 'https://bycard.by/'

    # Create topic if it doesn't exist
    admin_client = KafkaAdminClient(
        bootstrap_servers=f"{servers}:{port}",
        client_id='test'
    )
    if topic not in admin_client.list_topics():
        topic_list = [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=num_replicas)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    producer = KafkaProducer(
        bootstrap_servers=[f"{servers}:{port}"],
        value_serializer=json_serializer
    )
    # Collect data existing in Kafka
    existing_data = get_existing_data(topic, servers, port)
    # Parse web page and push to Kafka
    all_event_links = get_all_links_for_parsing(url=url)
    progress_bar = tqdm(total=len(all_event_links))
    for link in all_event_links:
        event_list = parse_event_by_link(link)
        if event_list:
            for el in event_list:
                if f"{el.get('name')}, {el.get('event_dt')}, {el.get('event_place')}" not in existing_data:
                    producer.send(topic=topic, value=el)
        else:
            print("No scheduled data for event - skipping")
        progress_bar.update(1)
    close_driver()


if __name__ == "__main__":
    main()
