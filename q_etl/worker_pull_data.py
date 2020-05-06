import asyncio
import datetime
from q_etl.etl import Pipeline
from q_etl.config import UPSTREAM_TOPIC


async def main():
	print('Starting worker...')
	worker_id = Pipeline.register_worker('worker_pull_data', 'localhost', datetime.datetime.now())
	await Pipeline.poll(worker_id, UPSTREAM_TOPIC)


if __name__ == '__main__':
	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())
