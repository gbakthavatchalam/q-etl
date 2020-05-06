import asyncio
import datetime
from .etl import Pipeline
from .config import DB_TOPIC


async def main():
	print('Starting worker...')
	worker_id = Pipeline.register_worker('worker_pull_data', 'localhost', datetime.datetime.now())
	print(f'Worker registered with id {worker_id}')
	Pipeline.poll(worker_id, DB_TOPIC)


if __name__ == '__main__':
	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())
