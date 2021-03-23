import pandas as pd
import sys
import os
import boto3
import json
from datetime import date
import requests
import logging
logging.basicConfig(stream=sys.stdout)
default_target_path = os.getcwd()

class data_preparation:
	
	def __init__(self, target_file_system, target_path = default_target_path, target_file_name = "currency_hist_records", s3_creds = None, s3_bucket = None):
		self.LOGGER = logging.getLogger(type(self).__name__)
		self.LOGGER.setLevel(logging.INFO)
		

		self.target_file_system = target_file_system

		if target_path is None:
			self.target_path = default_target_path

		if target_file_name is None:
			self.target_file_name = 'currency_hist_records'
			
		self.s3_creds = s3_creds
		self.s3_bucket = s3_bucket
		self.URL = "https://api.exchangeratesapi.io/history?start_at=2018-01-01"
		
		self.process()
		pass
	
	def process(self):
		
		hist_records = self.data_fetch()
		self.save_data(hist_records)
		self.filter_records(hist_records)
	
	def data_fetch(self):
		"""
			Method to fetch the historical records with date filters
			Returns - JSON - Historical records
			
			 - Gets the current date
			 - Creates URL to filter with end_dt 
			 - Sends get request to the URL
			 - Converts the returned response to JSON
			 
			 current_date - String ::: date_format -- YYYY-MM-DD
			 URL - String
			 historical_records_json - JSON 
			 
			
		"""
		current_date = date.today().strftime("%Y-%m-%d")
		master_URL = """{main_url}&end_at={end_dt_filter}""".format(main_url = self.URL, 
														   end_dt_filter = current_date)
		
		self.LOGGER.info("URL to fetch records --> {master_url}\n".format(master_url = master_URL))
		
		try:
			self.LOGGER.info("Get request --> hitting the URL to fetch data")
			response = requests.get(url = master_URL)
			self.LOGGER.info("Get request ::: Status --> Successful\n")
		except Exception as e:
			self.LOGGER.info("Get request ::: Status --> Failed")
			self.LOGGER.info("Reason ::: {failure_logs}".format(failure_logs = str(e)))
			raise 
			
		historical_records_json = response.json()
		self.LOGGER.info("Display sample records fetched from URL\n")
		print(historical_records_json['rates'][list(historical_records_json['rates'].keys())[0]])
		print(' ')
		
		return historical_records_json
	
	def s3_client(self):
		
		if self.s3_creds is not None:
			role_arn = self.s3_creds['RoleArn']
			ext_id = self.s3_creds['ExternalId']
			region = self.s3_creds['region_name']

			"""
				Note : The below part is tested using personal AWS account

				Method to create a S3 session using boto3

				- Requires -- > External ID and role_arn
				- Creates a AWS Security token service valid for 60 mins
				- Returns S3 session



			"""
			try:
				self.LOGGER.info("** set up S3 client from Boto3 **")
				AWS_sts = boto3.client('s3')
			except Exception as e:
				self.LOGGER.error("self.client = boto3.client('s3') exception :: " + str(e))     

			assumed_role_credentials = AWS_sts.assume_role(RoleArn=role_arn,
														   RoleSessionName="Readings3data", 
														   DurationSeconds=3600,
														   ExternalId=ext_id)['Credentials']

			s3_session = boto3.resource('s3',
								aws_access_key_id=assumed_role_credentials["AccessKeyId"],
								aws_secret_access_key=assumed_role_credentials["SecretAccessKey"],
								aws_session_token=assumed_role_credentials['SessionToken'],
								region_name=region)

			return s3_session
		
		else:
			self.LOGGER.info("S3 Creds are none, Please provide valid S3 Creds")
			self.LOGGER.info("Required S3 creds in list format :::: [RoleArn, ExternalId, region_name]")
			
			
	def structure_json(self, hist_records):
		new_hist_records=[]
		for dates in hist_records['rates']:
			for nos in hist_records['rates'][dates]:
				temp_dict={}
				temp_dict['date'] = dates
				temp_dict['currency'] = nos
				temp_dict['rate'] = hist_records['rates'][dates][nos]
				new_hist_records.append(temp_dict)
				
		return new_hist_records

				
	def save_data(self, hist_records):
		
		"""
			Method to save the historical records to Local driver or S3 in JSON format
			
			- If no target_path is provided --> Save location will be in CWD
			- Accepts two file systems --> Local, S3
		"""
		hist_records = self.structure_json(hist_records)
		
		file_destination = """{target_path}/{target_file_name}.json""".format(target_path = self.target_path,
																			 target_file_name = self.target_file_name)
		
		if self.target_file_system.upper() == "LOCAL" or self.target_file_system.upper() == "LOCAL_DRIVE" or self.target_file_system.upper() == "LOCAL_STORAGE":
			try:
				self.LOGGER.info("Pushing JSON file to Local drive - Path ::: {file_dstn}\n".format(file_dstn=file_destination))
				
				with open(file_destination , 'w') as out_file:
					json.dump(hist_records, out_file)   
				self.LOGGER.info("Status ::: Succcessful --> Currency historical records Path ::: {file_dstn}\n".format(file_dstn=file_destination))

			except Exception as e:
				self.LOGGER.info("Status ::: Failed saving file --> Path ::: {file_dstn}\n".format(file_dstn=file_destination))
				self.LOGGER.info("Error -->" + str(e))
				
		elif self.target_file_system.upper() == "S3":
			
			s3_session = self.s3_client()
			
			ServerSideEncryption = self.s3_creds['ServerSideEncryption']
			SSEKMSKeyId = self.s3_creds['SSEKMSKeyId']
			
			file_destination = """{target_path}/{target_file_name}.json""".format(target_path = self.target_path,
																					 target_file_name = self.target_file_name)
			if self.s3_bucket is not None:
				try:
					self.LOGGER.info("Putting JSON file to S3")
					self.LOGGER.info("S3 Location details --> {file_destination}".format(file_destination = file_destination))

					s3_session.Object(self.s3_bucket,
									  file_destination).put(Body = json.dumps(hist_records),
														   ServerSideEncryption=ServerSideEncryption,
															SSEKMSKeyId=SSEKMSKeyId)
					
					self.LOGGER.info("put_to_s3 Status --> Successful")
					
				except Exception as e:
					self.LOGGER.info("Error pushing data to S3 --> {error}".format(error = str(e)))
					
					
	def filter_records(self, hist_records):

		self.LOGGER.info("Filtering historical data for currencies")
		
		master_filtered_records = {}
		filtered_records = {}
		hist_records_dates = list(hist_records['rates'].keys())
		
		# Changing the IND to INR to match the currency names in JSON
		
		required_currencies = ['EUR', 'USD', 'JPY', 'CAD', 'GBP', 'NZD','INR']

		self.LOGGER.info("Changing IND to INR to match the value in the JSON")
		self.LOGGER.info("Required_currencies" + str(required_currencies))

		for dates in hist_records_dates:
			avail_curr_filtered = {}
			for avail_curr in hist_records['rates'][dates]:
				if avail_curr in required_currencies:
					avail_curr_filtered[avail_curr] = hist_records['rates'][dates][avail_curr]
	
			filtered_records[dates]=avail_curr_filtered
	
									
		master_filtered_records['rates'] = filtered_records
		self.LOGGER.info("Status successful --> Records filtered on {required_currencies}".format(required_currencies = required_currencies))
