from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import pandas as pd
import os
import logging
import sys

spark = SparkSession \
        .builder \
        .appName("PushToPsql") \
        .config("spark.jars", os.getcwd()+"/postgresql-42.2.19.jar") \
        .getOrCreate()
logging.basicConfig(stream=sys.stdout)

class data_processing:
    
    def __init__(self, json_read_path, psql_creds):
        
        """ 
            psql_creds - dict
                - databasename, username, password
        """
        if json_read_path is None:
        	self.json_read_path = os.getcwd()

        self.psql_creds = psql_creds
        self.LOGGER = logging.getLogger(type(self).__name__)
        self.LOGGER.setLevel(logging.INFO)
        self.process()
        pass
    
    def process(self):
        df = self.json_to_spark_df()
        self.create_currency_hist_tables(df)
        self.push_to_psql()
    
    def json_to_spark_df(self):
        
        path = """{json_read_path}/currency_hist_records.json""".format(json_read_path = self.json_read_path)
        
        try:
            self.LOGGER.info("SPARK df --> Reading JSON file from path")
            self.LOGGER.info(path)
            historical_rec_df = spark.read.option("multiLine","true").json(path)
            self.LOGGER.info("Status --> Successfully\n")
        except Exception as e:
            self.LOGGER.info("Status --> Failed")
            self.LOGGER.info("Error ::: " + str(e))
            raise
        
        # Remove duplicates from df
        self.LOGGER.info("Removing duplicate values from df")
        historical_rec_df = historical_rec_df.select('date','currency','rate').distinct()
        self.LOGGER.info("Printing the df ")
        historical_rec_df.show(5)
        
        return historical_rec_df
        
    def create_currency_hist_tables(self, df):
        
        self.currency_df = df.select('currency')
        self.historical_data_df = df
        
        
    def push_to_psql(self):
        
        databasename = self.psql_creds['databasename']
        username = self.psql_creds['username']
        password = self.psql_creds['password']
        host = self.psql_creds['host']

        mode = "overwrite"
        url = "jdbc:postgresql://{host}/{databasename}".format(databasename = databasename, host = host)
        properties = {"user": username,
                      "password": password,
                      "driver": "org.postgresql.Driver"}
        
        
        try:
            self.LOGGER.info("Pushing the tables to Postgres SQL using JDBC Pyspark")
            self.currency_df.write.jdbc(url=url, 
                                          table="currency_df", 
                                          mode=mode, 
                                          properties=properties)

            self.historical_data_df.write.jdbc(url=url, 
                                          table="historical_data_df", 
                                          mode=mode, 
                                          properties=properties)

            self.LOGGER.info("Pushed the tables successfully")
            
        except:
            self.LOGGER.info("JDBC pyspark --> failed")
            self.LOGGER.info("Using Python to push tables to Postgres SQL")
            URL = "postgresql://{user}:{password}@{host}/{databasename}".format(user=username,
                                                                                       password = password,
                                                                                       databasename = databasename,
                                                                                       host = host)
            
            engine = create_engine(URL)
            
            # Writing the results to psql
            try:
                currency_df = self.currency_df.toPandas()
                historical_data_df = self.historical_data_df.toPandas()
                self.LOGGER.info("Pushing files to pssql using python sqlalchemy module")
                
                currency_df.to_sql('currency_df', 
                                   engine,  
                                    if_exists='replace')
                
                historical_data_df.to_sql('historical_data_df', 
                                          engine,  
                                          if_exists='replace')
                
                
                self.LOGGER.info("Status --> Successful")
            except Exception as e:
                self.LOGGER.info("Status --> Failed" + str(e))
               
