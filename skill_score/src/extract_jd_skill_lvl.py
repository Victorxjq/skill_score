# -*- coding: utf-8 -*-

import argparse
import codecs
import json

import pandas as pd
import pygtrie
import regex as re
from pyspark import SparkContext

import zlib
import binascii
import json


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

with open("../data/skill_valid_v0x9", "r", encoding="utf-8") as f:
    skill_words = [x.replace("\n", "") for x in f.readlines()]

t = pygtrie.CharTrie()
for k in skill_words:
    t[k.lower()] = k.lower()


def get_longest_skill_words(input_text):
    if not isinstance(input_text, str):
        return []
    input_text = input_text.lower()
    start = 0
    len_text = len(input_text)
    rt_list = []
    while start < len_text:
        text_cut = input_text[start:len_text]
        tmp = t.longest_prefix(text_cut)
        if tmp[0]:
            rt_list.append(tmp[0])
            start += len(tmp[0])
        else:
            start += 1
    return rt_list


def get_merge_skill_words(input_text):
    if not isinstance(input_text, str):
        return []
    input_text = input_text.lower()
    start = 0
    len_text = len(input_text)
    rt_list = []
    end = 0
    while start < len_text:
        text_cut = input_text[start:len_text]
        tmp = t.longest_prefix(text_cut)
        if tmp[0]:
            if start == end and rt_list:
                rt_list[-1] = rt_list[-1] + tmp[0]
            else:
                rt_list.append(tmp[0])
            end = start + len(tmp[0])
            start += len(tmp[0])
        else:
            start += 1
    return rt_list


def judge_pure_english(keyword):
    return all(ord(c) < 128 for c in keyword)


def split_zh_in(s):
    text = [""]
    for item in list(s):
        if judge_pure_english(item):
            if judge_pure_english(text[-1]):
                text[-1] = text[-1] + item
            else:
                text.append(item)
        else:
            text.append(item)
    return text


def get_skill_words_back(input_text):
    if not isinstance(input_text, str):
        return []
    input_text = input_text.lower().replace("、", "")
    input_text = split_zh_in(input_text)
    len_text = len(input_text)
    start = len_text
    rt_list = []
    end = len_text
    end_tmp = len_text
    pair = []
    while end > -1:
        text_cut = ''.join(input_text[end:len_text])
        tmp = t.longest_prefix(text_cut)
        if tmp[0] and len(tmp[0]) > 1:
            if judge_pure_english(tmp[0]):
                if len(tmp[0]) < len(input_text[end]):
                    tmp_len = -1
                else:
                    tmp_len = 1
            else:
                tmp_len = len(tmp[0])
            if (start - end) <= tmp_len and rt_list:
                tool_t = rt_list[-1]
                if rt_list[-1] not in tmp[0]:
                    rt_list[-1] = ''.join(input_text[end:end_tmp])
                else:
                    rt_list[-1] = tmp[0]
                if rt_list[-1].endswith(tool_t):
                    pair.append((rt_list[-1], tool_t))
            else:
                rt_list.append(tmp[0])
            start = end
            end_tmp = end + len(rt_list[-1])
            end -= 1
        else:
            end -= 1
    return [rt_list, pair]


class RegexExtractor(object):
    def __init__(self):
        pass

    def __call__(self, data):
        line = data.get("data")
        if not line:
            return []
        else:
            line = str(line).strip().lower()
        res = []
        rule = re.compile(u"((熟练|熟悉|精通|了解|擅长|熟习|知道|理解|熟知|参与|具有|具备|掌握|应用|运用|使用)+)([\\s\\S]*?)[。|;|；|!|\\n]")
        tmp = dict()
        tmp["cv_tag"] = json.loads(data.get("cv_tag").get("cv_tag"))
        tmp["id"] = data.get("id")
        tmp["skill_lvl_pair"] = []
        rt = rule.findall(line)
        for x in rt:
            prefix = x[0]
            skill_list = get_longest_skill_words(x[2])
            tmp["skill_lvl_pair"].extend([[x, prefix] for x in skill_list])
        res.append(tmp)
        return res


g_tool_regex_extractor = RegexExtractor()


# class CVFieldExtractor(object):
#     def __init__(self, fields):
#         self.fields = fields
#
#     def __call__(self, data):
#         res = {field: data.get(field, None) for field in self.fields}
#         return res


# g_cv_desc_req_extractor = CVFieldExtractor(['cv_tag'])


def decode_escape(_line):
    _line = codecs.decode(_line, "unicode_escape")
    return _line


def extract_cv_info(line):
    # data = json.loads(line[1])
    # res = []
    # for v in g_cv_desc_req_extractor(data).values():
    #     if v:
    #         res.extend(re.split('\n', v))
    result = [{"id":eval(line)[0], "cv_tag":eval(line)[1][1], "data": eval(line)[1][0]}]
    return result


def get_match_sentence(extract_cv_skill):
    assert isinstance(extract_cv_skill, dict)
    result = []
    if extract_cv_skill:
        skill_lvl_pair = extract_cv_skill.get("skill_lvl_pair")
        if extract_cv_skill.get("cv_tag"):
            work_id=list(extract_cv_skill["cv_tag"].keys())
            if extract_cv_skill.get("cv_tag").get(extract_cv_skill["cv_tag"].keys()[0]):
                function_name ='test'
                print(extract_cv_skill.get("cv_tag").get(work_id[0]).get("should")[0].split(':')[0])
                function_id = extract_cv_skill.get("cv_tag").get(work_id[0]).get("should")[0].split(':')[0]
                if function_name:
                    function_name = decode_escape(function_name)
                    result = [[json.dumps({function_id: function_name}, ensure_ascii=False),
                               json.dumps(x, ensure_ascii=False)] for x in skill_lvl_pair]
    return result


def spark_regex_extract_tools(input_path, output_path):
    sc = SparkContext(appName='spark_regex_extract_tools')
    sc.textFile(input_path) \
        .flatMap(extract_cv_info) \
        .flatMap(lambda line: g_tool_regex_extractor(line)) \
        .map(lambda x: json.dumps(x)) \
        .saveAsTextFile(output_path)
    sc.stop()


# def spark_extract_jd_fields(input_path, output_path, fields):
#     extractor = CVFieldExtractor(fields)
#     sc = SparkContext(appName='spark_extract_jd_fields')
#     sc.textFile(input_path) \
#         .map(lambda line: uncompress(line.split('\t')[1])) \
#         .map(lambda line: json.loads(line)) \
#         .map(extractor) \
#         .map(lambda data: json.dumps(data)) \
#         .coalesce(100) \
#         .saveAsTextFile(output_path)
#     sc.stop()


def agg_skill_level(line):
    skill_rank = line[2]
    skill_lvl_dict = {}
    for x in skill_rank:
        if x[0]:
            skill_lvl_item = x[0]
            skill_item = skill_lvl_item[0]
            skil_lvl = skill_lvl_item[1]
            skill_lvl_ct = x[1]
            if skill_item not in skill_lvl_dict.keys():
                skill_lvl_dict[skill_item] = [[skil_lvl, skill_lvl_ct]]
            else:
                skill_lvl_dict[skill_item].append([skil_lvl, skill_lvl_ct])
    # if skill_lvl_dict:
    #     rt = [line[0], line[1], dict(sorted(skill_lvl_dict.items(), key=lambda x: x[1][1], reverse=True))]
    # else:
    #     rt = [line[0], line[1]]
    rt = [line[0], line[1], skill_lvl_dict]
    return rt


def spark_extract_skill_level(input_path, output_path):
    sc = SparkContext(appName='spark_extract_skill_level')

    sc.textFile(input_path) \
        .flatMap(extract_cv_info) \
        .flatMap(lambda line: g_tool_regex_extractor(line)) \
        .flatMap(get_match_sentence) \
        .map(lambda data: (data[0], (1, data[1]))) \
        .aggregateByKey((0, ""), lambda acc, val: (acc[0] + val[0], acc[1] + "|&|" + val[1]),
                        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + "|&|" + acc2[1])) \
        .map(lambda data: [data[0], data[1][0],
                           sorted(list(pd.value_counts(data[1][1].split("|&|")).items()), key=lambda x: x[1],
                                  reverse=True)]) \
        .sortBy(lambda x: x[1], False) \
        .map(lambda data: [json.loads(data[0]), data[1], [[json.loads(x[0]), x[1]] for x in data[2] if x[0] != ""]]) \
        .map(lambda data: agg_skill_level(data)) \
        .map(lambda data: json.dumps(data, ensure_ascii=False)) \
        .coalesce(30) \
        .saveAsTextFile(output_path)

    sc.stop()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest='command')

    regex_extract_tools_parser = subparsers.add_parser('regex_extract_tools')
    regex_extract_tools_parser.add_argument('--input_path', required=True)
    regex_extract_tools_parser.add_argument('--output_path', required=True)

    extract_jd_fields_parser = subparsers.add_parser('extract_jd_fields')
    extract_jd_fields_parser.add_argument('--input_path', required=True)
    extract_jd_fields_parser.add_argument('--output_path', required=True)
    extract_jd_fields_parser.add_argument('--fields', action='append')

    extract_skill_words_parser = subparsers.add_parser('extract_skill_words_sentence')
    extract_skill_words_parser.add_argument('--input_path', required=True)
    extract_skill_words_parser.add_argument('--output_path', required=True)

    args = parser.parse_args()

    if args.command == 'regex_extract_tools':
        spark_regex_extract_tools(args.input_path, args.output_path)
    # elif args.command == 'extract_jd_fields':
    #     spark_extract_jd_fields(args.input_path, args.output_path, args.fields)
    elif args.command == 'extract_skill_words_sentence':
        spark_extract_skill_level(args.input_path, args.output_path)
