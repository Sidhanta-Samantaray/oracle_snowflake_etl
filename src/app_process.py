#!/usr/bin/python
import sys
import os
import threading
import logging
import csv
import yaml
import oracle_utility as app_oracle
import file_utility
import time
from  datetime import datetime
import snowflake_utility 

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

'''
envFile="configs/app.yaml"

with open(envFile, 'r') as stream:
    data_loaded = yaml.load(stream,Loader=yaml.FullLoader)
'''
file_inbox_dir=None
  


'''
Method invoked from Main Script
Internally Creates Multiple Threads ( based on no. of batches)
'''
def batch_process(batches,filePrefix,query):
    global batchProcessStatus
    batchProcessStatus=True
    start_timestamp = datetime.now()
    logger.info(query)        
    batchThreads=[]    
    for batch in batches:
        batchCount=batch['batchId']    
        currentStart=batch['currentStart']
        currentEnd=batch['currentEnd']
        #Submit a Thread to invoke the process method that will do the actual processing
        t=threading.Thread(name="batch_"+str(batchCount),target=process,args=(batchCount,currentStart,currentEnd,filePrefix,query))
        batchThreads.append(t)        
        logger.info(t.name+" started")
        logger.info(batch)
        t.start()
    #Wait for all the batches to complete    
    for t in batchThreads:
        t.join()
        logger.info(t.name+" completed")        
    end_timestamp=  datetime.now()
    logger.info("Time Taken to Process All Threads "+str(end_timestamp-start_timestamp))
    return batchProcessStatus    
'''
Method does below activities in sequence
        1- Read from Oracle
        2- Append to File
'''
def process(batchCount,startValue,endValue,filePrefix,query):
    start_timestamp = datetime.now()
    try:    
        threadName=threading.current_thread().name
        logger.info("Thread Name:"+threadName+":["+str(startValue)+","+str(endValue)+"]"+"Started at "+str(start_timestamp))
        db=app_oracle.get_db_connection()
        sqlString= query
        extractCursor=app_oracle.get_db_cursor(db)
        extractCursor.execute(sqlString, {"startValue":startValue, "endValue":endValue})
        while True:
                rows=extractCursor.fetchmany()
                if not rows:
                        break
                export_csv(rows,filePrefix)#Method appends the rows in a file
        extractCursor.close()
        db.close()
    except Exception as e:
         logger.error(e)
         batchProcessStatus=False    
    end_timestamp=  datetime.now()
    logger.info("Thread Name:"+threadName+":["+str(startValue)+","+str(endValue)+"]"+"Completed at "+str(end_timestamp))
    logger.info("Time Taken for Thread Name:"+threadName+" "+str(end_timestamp-start_timestamp))
    

'''
        Method accepts the rows and filePrefix then open a file in append mode and adds the new rows to file
'''
def export_csv(rows,filePrefix):
    try:    
        threadName=threading.current_thread().name
        filename=filePrefix+"_"+str(threadName)+".csv"
        csv_file = open(file_inbox_dir+filename, "a")
        writer = csv.writer(csv_file,dialect=csv.excel, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
        #print("File:-"+filename+" Started")
        writer.writerows(rows)
        csv_file.close()
        logger.info(str(len(rows)) +" rows added to "+filename)   
    except Exception as  e:
        logger.error(e)        

def split_file_process(file_inbox_dir,file_work_dir):
    fileList=file_utility.get_file_list(file_inbox_dir)
    threads=[]
    for f in fileList:
        t=threading.Thread(target=file_utility.file_split,args=(f,file_work_dir))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()    

def merge_file_process(file_inbox_dir,file_work_dir):
    fileList=file_utility.merge_file_list(file_inbox_dir)
    file_utility.merge_csv(fileList,file_work_dir+"file_merged.csv")

def compress_files(file_inbox_dir,file_work_dir):
     start_timestamp = datetime.now()
     threads=[]
     try:   
        fileList=os.listdir(file_inbox_dir)
       
        for f in fileList:
                threads=[]
                source=file_inbox_dir+f    
                destination=file_work_dir+f+".gz"
                t=threading.Thread(target=file_utility.gzip_compress_file,args=(source,destination))
                threads.append(t)
                t.start()
     except Exception as e:
        logger.error(e)
     for t in threads:
         t.join()   
     end_timestamp=  datetime.now()
     logger.info("Time Taken for Compressing Files:"+str(end_timestamp-start_timestamp))      

'''
Wrapper Method to Call Snowflake PUT and COPY
'''
def publish_snowflake(path,tableName):
        start_timestamp= datetime.now()
        path=path+"*"
        '''
        threads=[]
        for f in os.listdir(path):
            fileName=str(path+f)
            t=threading.Thread(target=snowflake_utility.put_file_to_stage,args=(fileName,tableName))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()            
        '''
        snowflake_utility.put_file_to_stage(path,tableName)
        end_timestamp= datetime.now()
        logger.info("Time Taken for PUT Files Operations:"+str(end_timestamp-start_timestamp))      
        
        start_timestamp= datetime.now()
        snowflake_utility.copy_into_table(tableName)
        end_timestamp= datetime.now()
        logger.info("Time Taken for COPY Operations:"+str(end_timestamp-start_timestamp))     