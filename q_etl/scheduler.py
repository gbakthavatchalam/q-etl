import uuid
import json

from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092', 'group.id': 'mygroup'})
TOPIC = "batch"


class ProducerError(Exception):
	pass


def produce(topic, key, value):
	p.produce(topic, key=key, value=json.dumps(value).encode())
	p.poll(0.5)


def main():
	messages = [
		{
			"connector": "api_holiday",
			"resources": [
				{
					"name": "HolidayAPI",
					"method": "get",
					"params": {
						"country": "all",
						"year": 2019
					}
				}
			]
		}
	]
	for message in messages:
		produce(TOPIC, key=str(uuid.uuid4()).encode(), value=message)

main()