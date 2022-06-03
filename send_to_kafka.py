from kafka import KafkaProducer, KafkaConsumer
import json
from tqdm import tqdm
import argparse


KAFKA_ADR = 'localhost:9092'
TOPIC = 'events1'


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(
    bootstrap_servers=[KAFKA_ADR],
    value_serializer=json_serializer
)


def get_existing_data():
    existing_data = []
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_ADR],
        auto_offset_reset='earliest',
        consumer_timeout_ms=2000
    )
    for msg in consumer:
        msg_data = json.loads(msg.value)
        existing_data.append(f"{msg_data.get('name')}, {msg_data.get('event_dt')}, {msg_data.get('event_place')}")
    return existing_data


def main(src_name):
    if src_name == 'ticketpro':
        from ticketpro import get_all_links_for_parsing, parse_event_by_link, close_driver
        url = 'https://www.ticketpro.by/'
    elif src_name == 'bycard':
        from bycard import get_all_links_for_parsing, parse_event_by_link, close_driver
        url = 'https://bycard.by/'
    existing_data = get_existing_data()
    all_event_links = get_all_links_for_parsing(url=url)
    progress_bar = tqdm(total=len(all_event_links))
    for link in all_event_links[:5]:
        event_list = parse_event_by_link(link)
        if event_list:
            for el in event_list:
                if f"{el.get('name')}, {el.get('event_dt')}, {el.get('event_place')}" not in existing_data:
                    producer.send(topic=TOPIC, value=el)
        else:
            print("No scheduled data for event - skipping")
        progress_bar.update(1)
    close_driver()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', type=str, default='bycard')
    opt = parser.parse_known_args()[0]
    main(opt.src)
