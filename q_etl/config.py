import os


LOGLEVEL = os.getenv("LOGLEVEL", "INFO")
LOGFORMAT = '%(asctime)s:%(module)s:%(lineno)s:%(levelname)-8s: %(message)s'

UPSTREAM_TOPIC = 'scripts'
DB_TOPIC = 'db'
ERROR_TOPIC = 'error'

KAFKA_HOST = 'localhost'
KAFKA_PORT = 9092
KAFKA_GROUP = 'mygroup'


MONITOR_HOST = os.getenv('MONITOR_HOST', '127.0.0.1')
MONITOR_PORT = os.getenv('MONITOR_PORT', 8000)