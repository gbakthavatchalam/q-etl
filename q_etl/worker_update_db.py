import asyncio
import datetime
from q_etl.etl import Pipeline
from q_etl.config import DB_TOPIC


async def main():
	print('Starting worker...')
	pipeline = Pipeline()
	worker_id = pipeline.register_worker('worker_pull_data', 'localhost', datetime.datetime.now())
	print(f'Worker registered with id {worker_id}')
	await pipeline.poll(worker_id, DB_TOPIC)


if __name__ == '__main__':
	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())
