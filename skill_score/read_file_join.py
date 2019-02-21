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


def iterate_r(iterable):
    r = []
    for v1_iterable in iterable:
        for v2 in v1_iterable:
            r.append(v2)

    return tuple(r)


def get_files_list(root_path):
    files_list = []
    for dir_path in os.popen("""hadoop dfs -ls %s | awk  -F ' '  '{print $8}' """ % (root_path)).readlines():
        dir_path = dir_path.strip()
        if len(dir_path) != 0:
            file_path_list = os.popen("""hadoop dfs -ls %s | awk  -F ' '  '{print $8}' """ % (dir_path)).readlines()
            for file_path in file_path_list:
                if file_path != dir_path:
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
    if len(k_id) < 15:
        try:
            info = json.loads(line[1])
            if 'cv_tag' in info.keys() and info['cv_tag'] != '':
                # print(info['cv_tag'])
                res = (k_id, {'cv_tag': info['cv_tag']})
            else:
                res = 'null'
        except:
            res = 'null'
    else:
        res = 'null'
    return res


def extract_cv_info_basic(line):
    line = line.split('\t')
    k_id = line[0]
    if len(k_id) < 15:
        try:
            info = json.loads(uncompress(line[1]))
            work_content=''
            skill_content=''
            if 'work' in info.keys() and info['work'] != '':
                work_content=info['work']
            if 'skill' in info.keys() and info['skill'] != '':
                skill_content = info['skill']
                # print(info['cv_tag'])
                res = (k_id, {'work': work_content,'skill':skill_content})
            else:
                res = 'null'
        except:
            res = 'null'
    else:
        res = 'null'
    return res


def add(a, b):
    return str(a) + '\t' + str(b)


# load data
if __name__ == '__main__':
    sc = SparkContext(appName='join_cv_all')
    algorithm_file_path = '/basic_data/icdc/algorithms/20190115/*'
    basic_file_path = '/basic_data/icdc/resumes_extras/20190115/*'
    print('start load algorithm files')
    inp_algo = sc.textFile(algorithm_file_path).map(extract_cv_info_algorithm).filter(lambda x: x != 'null' or x != [])
    print('start load basic files')
    inp_basic = sc.textFile(basic_file_path).map(extract_cv_info_basic).filter(lambda x: x != 'null' or x != [])
    print('Group_by_keys:')
    output_path = '/user/kdd_xijunquan/cv_skill_score/icdc_with_skill'
    result = inp_basic.join(inp_algo).saveAsTextFile(output_path)
    print('batch,completed')
    sc.stop()
