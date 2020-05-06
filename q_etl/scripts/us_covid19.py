import asyncio
import aiohttp
import datetime
import json
import logging


logger = logging.getLogger(__name__)


def parse_row(row):
	item = {
		'state_code': row['state'],
		'state_name': row['state'],
		'total_affected': row['positive'],
		'hospitalized_currently': row.get('hospitalizedCurrently', 0),
		'in_icu_currently': row['inIcuCurrently'],
		'on_ventilator_currently': row['onVentilatorCurrently'],
		'total_recovered': row['recovered'],
		'total_deaths': row['death'],
		'total_tests': row['totalTestResults']
	}
	if 'date' in row:
		item['date'] = str(datetime.datetime.strptime(str(row['date']), '%Y%m%d'))
	return item


def filter_rows(rows, lag):
	result = []
	for row in rows:
		dt = datetime.datetime.strptime(str(row['date']), '%Y%m%d')
		if dt.date() >= datetime.date.today() - datetime.timedelta(days=lag):
			result.append(row)
	return result


async def get_state_summary():
	url = 'https://covidtracking.com/api/states'
	table = 'us_covid19_summary_by_state'
	async with aiohttp.ClientSession() as session:
		async with session.get(url) as response:
			try:
				result = json.loads(await response.text())
				parsed = [parse_row(row) for row in result]
				return {'table': table, 'refresh': True, 'data': parsed}, None
			except Exception as e:
				return {}, str(e)


async def get_state_day_wise(lag):
	url = 'https://covidtracking.com/api/states/daily'
	table = 'us_covid19_daily_by_state'
	async with aiohttp.ClientSession() as session:
		async with session.get(url) as response:
			try:
				result = json.loads(await response.text())
				if lag > 0:
					result = filter_rows(result, lag)
				parsed = []
				for row in result:
					try:
						parsed.append(parse_row(row))
					except KeyError:
						logger.error(f"Data error {row}")
				return {'table': table, 'refresh': False, 'data': parsed}, None
			except Exception as e:
				return {}, str(e)


async def ingest_covid_data(lag=-1):
	summary, summary_error = await get_state_summary()
	day_wise, day_wise_error = await get_state_day_wise(lag=lag)
	response = []
	return [summary, day_wise], summary_error or day_wise_error


if __name__ == "__main__":
	loop = asyncio.get_event_loop()
	loop.run_until_complete(get_state_summary())
