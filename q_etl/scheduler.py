import uuid
import json

from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092', 'group.id': 'mygroup'})
TOPIC = "job"


class ProducerError(Exception):
	pass


def produce(topic, key, value):
	p.produce(topic, key=key, value=json.dumps(value).encode())
	p.poll(0.5)


def main():
	message = {
		"method": "ingest_covid_data",
		"params": {}
	}
	produce(TOPIC, key=str(uuid.uuid4()).encode(), value=message)

main()