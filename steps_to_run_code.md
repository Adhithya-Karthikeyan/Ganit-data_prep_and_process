Steps to run:

1. Running in Airflow

	- Install airflow 
	- Run command in terminal "airflow scheduler"
	- Run command in terminal "airflow webserver"
	- Open "http://localhost:8080/" in browser
	- Dag name: data_prep_and_process
	- Trigger the dag


2. Running without airflow locally

		#############################################################################################################################

	- Open the script in Jupyter notebook for better understanding- data_preparation and data_processing
	- call data_preparation class 
		--> arguments required
					target_file_system - Mandatory (Local or S3)
					target_path -  not mandatory (used default path - CWD)
					target_file_name - not Mandatory (used default name - currency_hist_records)
					s3_creds -  Mandatory (only when target_file_system is S3)
					s3_bucket - Mandatory (only when target_file_system is S3)

		EX: data_preparation("local") 

			OR

		s3_creds = {"RoleArn":"AWS_RoleArn",
					"ExternalId" : "AWS_ExternalId",
					"region_name" : "AWS_region_name",
					"ServerSideEncryption" : "S3_ServerSideEncryption",
					"SSEKMSKeyId" : "S3_SSEKMSKeyId"}


		data_preparation("s3", "path_to_save_json", "some_target_file_name", s3_creds, s3_bucket_name)

		#############################################################################################################################

	- Once data_preperation class runs successfully call the data_processing class

		--> arguments required
					json_read_path - str - Mandatory 
					psql_creds - data structure : Dict - Mandatory 
						ex: 
						{"databasename":"hist_layer","username":"adhithyakarthikeyan","password":"1234"}

						Note : the databasename must be present in postgressql before passing it as argument

			EX: data_processing("path_where json got saved in data_preparation class", {"databasename":"hist_layer","username":"adhithyakarthikeyan","password":"1234"})


