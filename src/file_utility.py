import os
import glob
import logging
import pandas as pd
from fsplit.filesplit import FileSplit
import gzip
import shutil

fileSizeThreshold=1024*1024
fileSplitSizeThreshold=180*1024*1024

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def merge_csv(fileNames,path):
    print(fileNames)
    #combine all files in the list
    combined_csv = pd.concat([pd.read_csv(f,low_memory=False) for f in fileNames ])
    #export to csv
    combined_csv.to_csv( path, index=False, encoding='utf-8-sig')

def merge_file_list(path):
    fileList=[]
    for f in os.listdir(path):
        size=os.path.getsize(path+f)
        if size<=fileSizeThreshold:
            fileList.append(path+f)
    return fileList

def file_split(fileName,outputDir):
    fs = FileSplit(file=fileName, splitsize=fileSplitSizeThreshold, output_dir=outputDir)
    fs.split()

def get_file_list(path):
    fileList=[]
    for f in os.listdir(path):
        size=os.path.getsize(path+f)
        if size>=fileSplitSizeThreshold:
            fileList.append(path+f)
    return fileList

def get_file_list_from_dir(path):
    return os.listdir(path)    

def gzip_compress_file(source,destination):
    with open(source, 'rb') as f_in:
        with gzip.open(destination, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)    