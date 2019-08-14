#!/usr/bin/python

import sys
import math
import logging
from  datetime  import datetime

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logging=logging.getLogger(__name__)



'''
    Needs below 3 numeric arguments
    1- Start of the Range
    2- End of the Range
    3- No of Batches
    Start should be < End
    No of Batch Should Be >0
    No of Batch Should be less than (End-Start)/2
    All the Batches can be processed in Paralllel
    Todo: Can be Enhanced to Pause the Parent Thread Until all the parallel Thread Processing is Completed
    Sample Call Example(Mac): python3 split-range.py 100 1007 5
    Sample Call Example(Windows): python split-range.py 100 1007 5
'''
'''
def processData(startNumber,endNumber):#Use to process each batch(Actual Task)
    print("Thread Name:",threading.current_thread().name,":[",startNumber,",",endNumber,"]","Started")
    time.sleep(1) #using to replicate Multi Thread Functionality - Not to be used in actual application
    print("Thread Name:",threading.current_thread().name,":[",startNumber,",",endNumber,"]","Completed")
'''

def split_batch(splitNum,rangeStart,rangeEnd,splitColumnType):
    if splitColumnType.upper() not in ["DATE","NUMBER"]:
        raise RuntimeError("In correct splitColumnType should be DATE or NUMBER")
    batches=[]
    logging.info("Start Range:"+str(rangeStart))
    logging.info("End Range:"+str(rangeEnd))
    logging.info("Split Into:"+str(splitNum))
    if str(splitColumnType).upper()=="DATE":
        rangeStart=datetime.fromisoformat(str(rangeStart)).timestamp()
        rangeEnd=datetime.fromisoformat(str(rangeEnd)).timestamp()
    if rangeStart>=rangeEnd:
        logging.error("In correct Parameter Start Range>=End Range")
        raise RuntimeError("In correct Parameter Start Range>=End Range")
    elif splitNum<1:
        logging.error("No Of Batches Should be greater than zero")
        raise RuntimeError("No Of Batches Should be greater than zero")
    elif splitNum>math.floor((rangeEnd-rangeStart)/2):
        logging.error("Invalid Range Count Should Be <(end-start)/2")
        raise  RuntimeError("Invalid Range Count Should Be <(end-start)/2")

    breakCount=(rangeEnd-rangeStart)/splitNum
    breakCount=math.floor(breakCount)
    currentStart=rangeStart
    currentEnd=rangeStart+breakCount
    rangeIndex=1

    if splitNum>1:#Apply the Split Logic if batch count >1
        while currentEnd<rangeEnd:
            #print("Range Start:",currentStart,"Range End:",currentEnd)
            batches.append(prepare_batch(rangeIndex,currentStart,currentEnd))
            currentStart=currentEnd+1 
            # Set Start and End+1
            currentEnd=currentStart+breakCount #Set End as Start+breakCount
            rangeIndex=rangeIndex+1
        #print("Range Start:",currentStart,"Range End:",rangeEnd)
        batches.append(prepare_batch(rangeIndex,currentStart,currentEnd))       
    if str(splitColumnType).upper()=="DATE":
        for batch in batches:
            batch['currentStart']=datetime.fromtimestamp(batch['currentStart'])
            batch['currentEnd']=datetime.fromtimestamp(batch['currentEnd'])
    logging.info(batches)
    return batches

def prepare_batch(batchCounter,currentStart,currentEnd):
    result=dict()
    result.__setitem__("batchId",batchCounter)
    result.__setitem__("currentStart",currentStart)
    result.__setitem__("currentEnd",currentEnd)
    return result