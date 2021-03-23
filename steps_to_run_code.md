**Steps to run:**

1. Running in Airflow

	- Install airflow 

	- Navigate to Ganit-data_prep_and_process/inputs
	- Open input.json file (This holds all the input configs to be passed to the jobs)
	- Edit the input.json and provide the required arguments

	**EX:**
		{
		"data_preparation_target_file_system" : "local",
		"data_preparation_target_path" : None,
		"data_preparation_target_file_name": None,
		"data_preparation_s3_creds" : None,
		"data_preparation_s3_bucket" : None,
		"data_processing_json_read_path" : None,
		"data_processing_psql_creds": {'databasename':'hist_layer','username':'user','password':'pass'}

		}


	- Run command in terminal "airflow scheduler"
	- Run command in terminal "airflow webserver"
	- Open "http://localhost:8080/" in browser
	- Dag name: data_prep_and_process
	- Trigger the dag


2. Running without airflow locally

		#############################################################################################################################

	- Open the script in Jupyter notebook for better understanding- data_preparation and data_processing
	- Call the data_preparation class 
		- **arguments required**
			- target_file_system - str-  Mandatory (Local or S3)
			- target_path - str -  not mandatory (used default path - CWD)
			- target_file_name - str- not Mandatory (used default name - currency_hist_records)
			- s3_creds -  str- Mandatory (only when target_file_system is S3)
			- s3_bucket - str- Mandatory (only when target_file_system is S3)

		- **EX:** data_preparation("local") 
		
			OR

		- s3_creds = {"RoleArn":"AWS_RoleArn",
					"ExternalId" : "AWS_ExternalId",
					"region_name" : "AWS_region_name",
					"ServerSideEncryption" : "S3_ServerSideEncryption",
					"SSEKMSKeyId" : "S3_SSEKMSKeyId"}


		 - data_preparation("s3", "path_to_save_json", "some_target_file_name", s3_creds, s3_bucket_name)

		#############################################################################################################################

	- Once data_preperation class runs successfully call the data_processing class
	- **Note** : the databasename must be present in postgressql before passing it as argument

		- **arguments required**
			- json_read_path - str - Mandatory 
			- psql_creds - data structure : Dict - Mandatory 
				- {"databasename":"hist_layer","username":"adhithyakarthikeyan","password":"1234"}


			- **EX:** data_processing("path_where json got saved in data_preparation class", {"databasename":"hist_layer","username":"adhithyakarthikeyan","password":"1234"})


