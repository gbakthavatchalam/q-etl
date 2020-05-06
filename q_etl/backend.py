import boto3
import botocore
import logging
import pandas as pd



logger = logging.getLogger()

OP_DIR = '/tmp'


class S3Backend:
	BUCKET = 'q-augment'

	def __init__(self, access_key, secret_key):
		logger.info("Initialising client using the given access key and secret key")
		self._client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

	def update(self, table, refresh, data):
		infile = f'{OP_DIR}/download/{table}.csv'
		opfile = f'{OP_DIR}/updated/{table}.csv'
		if not refresh:
			infile = f'{OP_DIR}/download/{table}.csv'
			opfile = f'{OP_DIR}/updated/{table}.csv'
			try:
				self._client.download_file(Bucket=self.BUCKET, Key=f'{table}.csv', Filename=infile)
				df = pd.read_csv(infile)
				df.append(data, True)
				df.to_csv(opfile, index=False)
			except botocore.exceptions.ClientError as e:
				df = pd.DataFrame(data)
				df.to_csv(opfile, index=False)
		else:
			df = pd.DataFrame(data)
			df.to_csv(opfile, index=False)
		self._client.upload_file(opfile, self.BUCKET, f'{table}.csv')


