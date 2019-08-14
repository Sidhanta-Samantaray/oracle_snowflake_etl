import snowflake.connector
import glob
import os
import time
import logging
from  datetime import datetime

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

snowflake_account:None
snowflake_db:None
snowflake_schema:None
snowflake_user:None
snowflake_password:None
snowflake_warehouse:None

def get_snowflake_connection():
    return snowflake.connector.connect (
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            database=snowflake_db,
            schema = snowflake_schema,
            warehouse=snowflake_warehouse
        )

        #Execute PUT command
def put_file_to_stage(filePath,stageName):
  
    try:
        put_command= "PUT file://"+filePath+" @%"+stageName
        logger.info("Put Command:-"+put_command)
        con=get_snowflake_connection()
        con.cursor().execute(put_command)
        con.close()
    except Exception as e:
        logger.error(e)      
        raise e

#Execute Copy Command
def copy_into_table(tableName):
    try:
        con=get_snowflake_connection()
        copy_command="COPY INTO "+tableName + " file_format=(format_name='CSV_NEW'NULL_IF=('NULL', 'NUL', '')) ON_ERROR='CONTINUE' "
        logger.info(copy_command)
        con.cursor().execute(copy_command)
        con.close()
    except Exception as e:
        logger.error(e)
        raise e   