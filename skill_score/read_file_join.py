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
    if len(k_id)<15:
        try:
            info = json.loads(line[1])
            if 'cv_tag' in info.keys() and info['cv_tag']!='':
                # print(info['cv_tag'])
                res = [(k_id, {'cv_tag': info['cv_tag']})]
            else:
                res = 'null'
        except:
            res ='null'
    else:
        res='null'
    return res


def extract_cv_info_basic(line):
    line = line.split('\t')
    k_id = line[0]
    if len(k_id) < 15:
        try:
            info = json.loads(uncompress(line[1]))
            if 'work' in info.keys() and info['work'] !='':
                # print(info['cv_tag'])
                res = [(k_id, {'work': info['work']})]
            else:
                res = 'null'
        except:
            res = 'null'
    else:
        res='null'
    return res


def add(a, b):
    return str(a) + '\t' + str(b)


# load data
if __name__ == '__main__':
    for val in range(0, 1):
        sc = SparkContext(appName='join_cv_all')
        algorithm_file_path = '/basic_data/icdc/algorithms/20190115/icdc_%s' % str(val)
        # print(algorithm_file_path)
        basic_file_path = '/basic_data/icdc/resumes_extras/20190115/icdc_%s' % str(val)
        print('start load algorithm files')
        index = 0
        # for alg_file_path in get_files_list(algorithm_file_path):
        for alg_file_path in ['/basic_data/icdc/algorithms/20190115/icdc_0/data__ff0f1b40_5207_4f3c_83d0_8f03b7185372']:
            cmd = 'hadoop fs -test -d %s' % alg_file_path
            if subprocess.call(cmd, shell=True) == 1:
                if len(alg_file_path) > 0:
                    if index == 0:
                        inp_all = sc.textFile(alg_file_path).map(extract_cv_info_algorithm).filter(lambda x:x!='null')
                        index += 1
                    else:
                        tmp = sc.textFile(alg_file_path).map(extract_cv_info_algorithm).filter(lambda x:x!='null')
                        inp_all = inp_all.union(tmp)
                        index += 1
        print('start load basic files')
        # for bas_file_path in get_files_list(basic_file_path):
        for bas_file_path in ['/basic_data/icdc/resumes_extras/20190115/icdc_0/data__ffd132e3_a01d_4ed3_bad0_98f9b8b069c4']:
            cmd = 'hadoop fs -test -d %s' % bas_file_path
            if subprocess.call(cmd, shell=True) == 1:
                if len(bas_file_path) > 0:
                    tmp = sc.textFile(bas_file_path).map(extract_cv_info_basic).filter(lambda x:x!='null')
                    inp_all = inp_all.union(tmp)
        print('Group_by_keys:')
        for val in inp_all.take(10):
            print(val)
        result = inp_all.sortByKey(ascending=True).reduceByKey(add)
        # result=inp_all.groupByKey().mapValues(list)
        print('save to txt:')
        # output_path='/user/kdd_xijunquan/cv_skill_score/test'
        output_path = '/user/kdd_xijunquan/cv_skill_score/icdc_%s' % str(val)
        # cmd = 'hadoop fs -test -d %s' % output_path
        # if subprocess.call(cmd, shell=True) == 1:
        #     subprocess.call('hadoop fs -rm -r %s' % output_path)
        for res in result.collect():
            print(res)
        # result.saveAsTextFile(output_path)
        print('batch %s,completed' % str(val))
        sc.stop()
