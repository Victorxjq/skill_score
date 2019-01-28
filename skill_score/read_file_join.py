# -*- coding=utf-8 -*-
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
import json
import os
import re
def get_files_list(root_path):
    files_list=[]
    for dir_path in os.popen("""hadoop dfs -ls %s | awk  -F ' '  '{print $8}' """ % (root_path)).readlines():
        dir_path = dir_path.strip()
        if len(dir_path) != 0:
            file_path_list = os.popen("""hadoop dfs -ls %s | awk  -F ' '  '{print $8}' """ % (dir_path)).readlines()
            for file_path in file_path_list:
                file_path = re.split(' ', file_path.replace('\n', ''))
                files_list.append(file_path[0])
    return files_list

algorithm_file_path='/basic_data/icdc/algorithms/20190115/'
basic_file_path='/basic_data/icdc/resumes_extras/20190115/'

def extract_cv_info(line):
    line=line.split('\t')
    id=line[0]
    info=json.loads(line[1])
    if 'cv_tag' in info.keys():
        res={id:{'cv_tag':info['cv_tag']}}
    else:
        res = {id: {'cv_tag': ''}}
    return res


#load data
if __name__ == '__main__':
    sc = SparkContext(appName='join_cv')
    test_path='/basic_data/icdc/algorithms/20190115/icdc_20/data__fcca1aa1_41ca_4df3_bf56_feea812b5d5d'
    sc.textFile(test_path).flatMap(extract_cv_info).collect()
    sc.stop()
