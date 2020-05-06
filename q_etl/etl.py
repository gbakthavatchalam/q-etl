import datetime
import json
import logging
import requests
import uuid

from confluent_kafka import Consumer, KafkaError, Producer

from .config import LOGFORMAT, LOGLEVEL, UPSTREAM_TOPIC, DB_TOPIC, ERROR_TOPIC, KAFKA_HOST, KAFKA_PORT, KAFKA_GROUP, MONITOR_HOST, MONITOR_PORT
from .register import registry
from .backend import S3Backend


logging.basicConfig(level=LOGLEVEL, format=LOGFORMAT)
logger = logging.getLogger()


settings = {
	'bootstrap.servers': f'{KAFKA_HOST}:{KAFKA_PORT}',
	'group.id': KAFKA_GROUP
}


p = Producer({'bootstrap.servers': f'{KAFKA_HOST}:{KAFKA_PORT}', 'group.id': KAFKA_GROUP})


class PipelineError(Exception):
	pass


class Pipeline:
	@property
	def tranid(self):
		return uuid.uuid4()

	@staticmethod
	async def call_job(tranid, message):
		logger.info(f'Processing transaction {tranid}')
		try:
			method = registry[message['method']]
			params = message['params'] or {}
			logger.info(f'Calling method {method} with params {params}')
			result, error = await method(**params)
			if error:
				Pipeline.write_to_error_queue(tranid, error)
				raise PipelineError(f'Error response from method {method}')
			else:
				Pipeline.write_to_db_queue(tranid, result)
		except KeyError:
			logging.error('Invalid method name')
			raise PipelineError('Invalid method name')
		except:
			logging.error(f'Unknown exception while processing tranid {tranid}')
			raise PipelineError(f'Unknown exception while processing tranid {tranid}')

	@staticmethod
	async def update_db(tranid, message):
		logging.info(f'Processing transaction {tranid}')
		try:
			for item in message:
				table = item['table']
				refresh = item['refresh']
				data = item['data']
				S3Backend.update(table, refresh, data)
		except Exception as e:
			logging.error(e)
			Pipeline.write_to_error_queue(tranid, item)


	@staticmethod
	async def log_error(tranid, message):
		logging.info(f'Processing transaction {tranid}')
		logging.error(message)
		#S3Backend.log_Error("transaction_error", False, message)

	@staticmethod
	def write_to_error_queue(tranid, error):
		p.produce(ERROR_TOPIC, key=tranid.encode(), value=json.dumps(error).encode())
		p.poll(0.5)

	@staticmethod
	def write_to_db_queue(tranid, result):
		p.produce(DB_TOPIC, key=tranid.encode(), value=json.dumps(result).encode())
		p.poll(0.5)

	@staticmethod
	async def poll(worker_id, topic):
		logger.info(f'Worker {worker_id} starts polling kakfa on topic {topic}')
		total_counter  = 0
		success_counter = 0
		_map = {
			UPSTREAM_TOPIC: Pipeline.call_job,
			DB_TOPIC: Pipeline.update_db,
			ERROR_TOPIC: Pipeline.log_error
		}
		c = Consumer(settings)
		c.subscribe([topic])
		while True:
			msg = c.poll(0.1)
			if msg is None:
				continue
			elif not msg.error():
				tranid = msg.key()
				message = json.loads(msg.value())
				handler = _map[topic]
				try:
					total_counter += 1
					await handler(tranid, message)
					success_counter += 1
				except PipelineError:
					pass
				Pipeline.update_worker(worker_id, datetime.datetime.now(), total_counter, success_counter)

	@staticmethod
	def register_worker(name, ip, inittime):
		url = f'http://{MONITOR_HOST}:{MONITOR_PORT}/worker/register'
		data = {'name': name, 'ip': ip, 'start_date': str(inittime)}
		response = requests.post(url, json=data)
		if response.status_code == 200:
			id_ = response.json()['id']
			logger.info(f'Worker registered with id {id_}')
			return id_

	@staticmethod
	def update_worker(worker_id, last_message_processed, total_processed, total_success):
		url = f'http://{MONITOR_HOST}:{MONITOR_PORT}/worker/{worker_id}/health'
		data = {"last_processed": last_message_processed, "total_processed": total_processed, "total_success": total_success}
		response = requests.put(url, json=data)
		if response.status_code == 200:
			logger.info(f'Worker registered with id {response.json()["id"]}')
