import datetime
from fastapi import FastAPI
from pydantic import BaseModel
import sqlite3


class Register(BaseModel):
	name: str
	ip: str
	start_date: datetime.datetime


class Health(BaseModel):
	last_processed: datetime.datetime
	total_processed: int
	total_success: int


app = FastAPI()

def setup_db():
	try:
		connect = sqlite3.connect("monitor.db")
		cursor = connect.cursor()
		cursor.execute('''CREATE TABLE worker (id integer primary key autoincrement, name varchar(50), ip varchar(20), start_date datetime)''')
		cursor.execute(
			'''CREATE TABLE health(worker_id integer primary key, last_processed datetime, total_processed integer, total_success integer)''')
		connect.commit()
		connect.close()
	except:
		pass


setup_db()


@app.post("/worker/register", )
def register(register: Register):
	connect = sqlite3.connect("monitor.db")
	cursor = connect.cursor()
	query = f'INSERT INTO worker (name, ip, start_date) VALUES ("{register.name}", "{register.ip}", "{register.start_date}")'
	cursor.execute(query)
	result = {'id': cursor.lastrowid}
	result.update(register.__dict__)
	connect.commit()
	connect.close()
	return result


@app.get("/worker/{worker_id}")
def get_worker(worker_id: int):
	connect = sqlite3.connect("monitor.db")
	cursor = connect.cursor()
	query = f'SELECT * FROM worker WHERE id={worker_id}'
	worker = cursor.execute(query).fetchone()
	query = f'SELECT * FROM health WHERE worker_id={worker_id}'
	health = cursor.execute(query).fetchone()
	resp = {
		"id": worker_id,
		"name": worker[1],
		"ip": worker[2],
		"start_date": worker[3],
		"total_success": health[3],
		"total_processed": health[2],
		"last_processed": health[1]
	}
	connect.close()
	return resp


@app.put("/worker/{worker_id}/health")
def update_health(worker_id: int, health: Health):
	connect = sqlite3.connect("monitor.db")
	cursor = connect.cursor()
	query = f'INSERT OR REPLACE INTO health (worker_id, last_processed, total_processed, total_success) ' \
			f'VALUES ({worker_id}, "{health.last_processed}", "{health.total_processed}", "{health.total_success}")'
	cursor.execute(query)
	connect.commit()
	connect.close()
	result = health.__dict__
	result['id'] = worker_id
	return result
