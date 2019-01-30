# -*- coding=utf-8 -*-
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
import json
import os
import re
import zlib
import binascii
import json
import subprocess


def uncompress(s):
    try:
        txt = zlib.decompress(binascii.unhexlify(s))
    except TypeError as e:
        txt = "{}"
    try:
        json_obj = json.loads(txt, strict=False)
        txt = json.dumps(json_obj, ensure_ascii=False)
        if (json_obj == None):
            txt = "{}"
    except ValueError as e:
        txt = "{}"
    return txt

def get_files_list(root_path):
    files_list = []
    for dir_path in os.popen("""hadoop dfs -ls %s | awk  -F ' '  '{print $8}' """ % (root_path)).readlines():
        dir_path = dir_path.strip()
        if len(dir_path) != 0:
            file_path_list = os.popen("""hadoop dfs -ls %s | awk  -F ' '  '{print $8}' """ % (dir_path)).readlines()
            for file_path in file_path_list:
                if file_path!=dir_path:
                    file_path = re.split(' ', file_path.replace('\n', ''))
                    files_list.append(file_path[0])
    return files_list

def get_files_list_single_layer(root_path):
    files_list = []
    for file_path in os.popen("""hadoop dfs -ls %s | awk  -F ' '  '{print $8}' """ % (root_path)).readlines():
        file_path = re.split(' ', file_path.replace('\n', ''))
        files_list.append(file_path[0])
    return files_list


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
        info = json.loads(uncompress(line[1]))
        if 'work' in info.keys():
            # print(info['cv_tag'])
            res = (k_id, {'work': info['work']})
        else:
            res = ''
    except:
        res = ''
    return res


# load data
if __name__ == '__main__':
    sc = SparkContext(appName='join_cv')
    algorithm_file_path = '/basic_data/icdc/algorithms/20190115/icdc_0/data__ff0f1b40_5207_4f3c_83d0_8f03b7185372'
    basic_file_path = '/basic_data/icdc/resumes_extras/20190115/icdc_0/data__ffd132e3_a01d_4ed3_bad0_98f9b8b069c4'
    print('start load algorithm files')
    index = 0
    # for file_path in get_files_list_single_layer(algorithm_file_path):
    for file_path in ['/basic_data/icdc/algorithms/20190115/icdc_0/data__ff0f1b40_5207_4f3c_83d0_8f03b7185372']:
        cmd='hadoop fs -test -d %s' % file_path
        if subprocess.call(cmd,shell=True)==1:
            if len(file_path)>0:
                if index == 0:
                    inp_all_algorithm = sc.textFile(file_path).flatMap(extract_cv_info_algorithm)
                    index += 1
                else:
                    tmp = sc.textFile(file_path).flatMap(extract_cv_info_algorithm)
                    inp_all_algorithm = inp_all_algorithm.union(tmp)
                    index += 1
    print('start load basic files')
    index = 0
    # for file_path in get_files_list_single_layer(basic_file_path):
    for file_path in ['/basic_data/icdc/resumes_extras/20190115/icdc_0/data__ffd132e3_a01d_4ed3_bad0_98f9b8b069c4']:
        cmd = 'hadoop fs -test -d %s' % file_path
        if subprocess.call(cmd, shell=True) == 1:
            if len(file_path) > 0:
                if index == 0:
                    inp_all_basic = sc.textFile(file_path).flatMap(extract_cv_info_basic)
                    index += 1
                else:
                    tmp = sc.textFile(file_path).flatMap(extract_cv_info_basic)
                    inp_all_basic = inp_all_basic.union(tmp)
                    index += 1
    inp_all_algorithm=inp_all_algorithm.filter((lambda x:x==''))
    inp_all_basic = inp_all_basic.filter((lambda x: x == ''))
    print('algorithm:')
    for val in inp_all_algorithm.collect():
        print(val)
    print('basic:')
    for val in inp_all_basic.collect():
        print(val)
    print('join task')
    for val in inp_all_algorithm.join(inp_all_basic).collect():
        print(val)
    print('completed')
    sc.stop()
