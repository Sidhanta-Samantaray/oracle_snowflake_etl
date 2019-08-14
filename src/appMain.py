#!/usr/bin/python

'''
#Todo- Refer commit time so that no records will be missed while reading the data 
Cleanup for Error out batches and re process
'''
import sys
import os
import yaml
import math
import argparse
import oracle_utility
import app_process
import snowflake_utility
from oracle_utility import find_split_range
from splitRange import split_batch
from app_process import batch_process,split_file_process,merge_file_process,compress_files,publish_snowflake
import time
from  datetime import datetime

start_timestamp = datetime.now()


try:
    #All the Command Line Arguments
    parser = argparse.ArgumentParser(description='Oracle CSV Export')
    parser.add_argument('--batchCount', help='No Of Batches', required=True)
    parser.add_argument('--maxBatchSize', help='Maximum Records in a Single DB Fetch', required=True)
    parser.add_argument('--filePrefix', help='Output file Name Prefix', required=True)
    parser.add_argument('--configFile', help='Configuration file Path', required=True)
    parser.add_argument('--snowflakeTable', help='Snowflake Table to Load', required=True)
    parser.add_argument('--splitQuery', help='Oracle Query to Determine the ranges', required=True)
    parser.add_argument('--splitColumnType', help='Data Type of Split Column can be NUMBER or DATE', required=False)
    parser.add_argument('--query', help='Oracle Query to Execute', required=True)
    args = vars(parser.parse_args())

    splitNum=int(args['batchCount'])
    splitQuery=str(args['splitQuery'])
    query=str(args['query'])
    filePrefix=str(args['filePrefix'])
    splitColumnType=str(args['splitColumnType'])

    if (not splitColumnType) and (splitColumnType.upper() not in ["DATE","NUMBER"]):
        raise RuntimeError("In correct splitColumnType should be DATE or NUMBER")

    envFile=str(args['configFile'])
    #if envFile is None:
    #    envFile="configs/app.yaml"

    #Read the Config File
    with open(envFile, 'r') as stream:
        data_loaded = yaml.load(stream,Loader=yaml.FullLoader)

    #oracle database properties
    oracle_utility.oracle_db_dns=str(data_loaded['oracle']['db'])
    oracle_utility.oracle_db_user=str(data_loaded['oracle']['userName'])
    oracle_utility.oracle_db_pass=str(data_loaded['oracle']['password'])
    oracle_utility.oracle_db_cursor_array_size=int(args['maxBatchSize'])

    #Snowflake Properties
    snowflake_utility.snowflake_account=str(data_loaded['snowflake']['account'])
    snowflake_utility.snowflake_db=str(data_loaded['snowflake']['database'])
    snowflake_utility.snowflake_schema=str(data_loaded['snowflake']['schema'])
    snowflake_utility.snowflake_user=str(data_loaded['snowflake']['user'])
    snowflake_utility.snowflake_password=str(data_loaded['snowflake']['password'])
    snowflake_utility.snowflake_warehouse=str(data_loaded['snowflake']['warehouse'])


    #Output directories
    file_inbox_dir=str(data_loaded['output']['inbox'])
    file_work_dir=str(data_loaded['output']['work'])
    app_process.file_inbox_dir=file_inbox_dir
   
    #Get the Ranges Based on the Split Query
    rangeResult=find_split_range(splitQuery)
    rangeStart=rangeResult['rangeStart']
    rangeEnd=rangeResult['rangeEnd']


    
    if type(rangeStart) is  datetime and type(rangeEnd) is datetime:
        splitColumnType="DATE"
    elif type(rangeStart) is int and type(rangeEnd) is int:    
        splitColumnType="NUMBER"
    else:
        raise RuntimeError("In correct splitColumnType should be DATE or NUMBER")    


    #Split the Ranges into batches based on the batchSize
    batches=split_batch(splitNum,rangeStart,rangeEnd,splitColumnType)


    #This is the where all the actual processing happens
    batchProcessStatus=batch_process(batches,filePrefix,query)

    if not batchProcessStatus:
        raise RuntimeError("Batch Process Error")    
    
    #Compress Files
    #compress_files(file_inbox_dir,file_work_dir)
    
    snowflakeTable=str(args['snowflakeTable'])
    #Publish to Snowflake
    publish_snowflake(file_inbox_dir,snowflakeTable)
    
except (KeyboardInterrupt,SystemExit,SystemError):
    print("Exit")
except Exception as e:
    print(e)    

end_timestamp=datetime.now()

print ("Main Program Completed in :-"+str(end_timestamp-start_timestamp))