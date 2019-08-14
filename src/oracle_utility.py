#!/usr/bin/python

import cx_Oracle
import logging
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


#database properties
oracle_db_dns:None
oracle_db_user:None
oracle_db_pass:None
oracle_db_cursor_array_size:None

#Acquire and return a db connection
def get_db_connection():
    db=cx_Oracle.connect(oracle_db_user,oracle_db_pass,oracle_db_dns)
    return db

#Return a DB Cursor
def get_db_cursor(db):
    cursor=db.cursor()
    cursor.arraysize=oracle_db_cursor_array_size
    return cursor

#Find the Ranges based on Splitquery and return the result
def find_split_range(splitQuery):
    start_timestamp = datetime.now()
    logging.info(splitQuery)
    rangeResult=dict()
    try:
        db=get_db_connection()
        cursor=db.cursor()
        cursor.execute(splitQuery)
        result=cursor.fetchone()
        rangeResult.__setitem__('rangeStart',result[0])
        rangeResult.__setitem__('rangeEnd',result[1])
    except Exception as e:
        logging.exception(e)
    cursor.close()
    db.close()
    end_timestamp=datetime.now()
    logger.info("Time Taken to Execute SplitQuery:-"+str(end_timestamp-start_timestamp))
    return rangeResult
