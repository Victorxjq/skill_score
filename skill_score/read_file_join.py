# -*- coding=utf-8 -*-
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
import json
import os
import re


def get_files_list(root_path):
    files_list = []
    for dir_path in os.popen("""hadoop dfs -ls %s | awk  -F ' '  '{print $8}' """ % (root_path)).readlines():
        dir_path = dir_path.strip()
        if len(dir_path) != 0:
            file_path_list = os.popen("""hadoop dfs -ls %s | awk  -F ' '  '{print $8}' """ % (dir_path)).readlines()
            for file_path in file_path_list:
                file_path = re.split(' ', file_path.replace('\n', ''))
                files_list.append(file_path[0])
    return files_list


algorithm_file_path = '/basic_data/icdc/algorithms/20190115/'
basic_file_path = '/basic_data/icdc/resumes_extras/20190115/'


def extract_cv_info_algorithm(line):
    line = line.split('\t')
    k_id = line[0]
    try:
        info = json.loads(line[1])
        if 'cv_tag' in info.keys():
            # print(info['cv_tag'])
            res = (k_id,{'cv_tag': info['cv_tag']})
        else:
            res = ''
    except:
        res = ''
    return res


def extract_cv_info_basic(line):
    line = line.split('\t')
    k_id = line[0]
    try:
        info = json.loads(line[1])
        if 'work' in info.keys():
            # print(info['cv_tag'])
            res = (k_id,{'work': info['work']})
        else:
            res = ''
    except:
        res = ''
    return res


# load data
if __name__ == '__main__':
    sc = SparkContext(appName='join_cv')
    test_path1='/basic_data/icdc/algorithms/20190115/icdc_0/data__ff0f1b40_5207_4f3c_83d0_8f03b7185372'
    test_path2='/basic_data/icdc/algorithms/20190115/icdc_0/data__bee3f589_238a_44da_9a6a_de34e589ff1b'
    test1=sc.textFile(test_path1).flatMap(extract_cv_info_algorithm)
    test2=sc.textFile(test_path2).flatMap(extract_cv_info_algorithm)
    test1=test1.union(test2)
    for x in test1.collect():
        print(x)
        if x=='':
            print('this is a null string')
    # index=0
    # for file_path in get_files_list(algorithm_file_path):
    #     if index==0:
    #         index+=1
    #         inp_all_algorithm=sc.textFile(file_path).flatMap(extract_cv_info_algorithm)
    #     else:
    #         tmp=sc.textFile(file_path).flatMap(extract_cv_info_algorithm)
    #         inp_all_algorithm=inp_all_algorithm.union(tmp)
    #         index+=1
    #
    # index=0
    # for file_path in get_files_list(basic_file_path):
    #     if index==0:
    #         index+=1
    #         inp_all_basic=sc.textFile(algorithm_file_path).flatMap(extract_cv_info_basic)
    #     else:
    #         tmp=sc.textFile(file_path).flatMap(extract_cv_info_basic)
    #         inp_all_basic=inp_all_basic.union(tmp)
    #         index+=1
    #
    # inp_all_algorithm.join(inp_all_basic).saveAsTextFile('/user/kdd_xijunquan/cv_skill_score/')
    sc.stop()
