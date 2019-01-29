"""

"""
import json
import math

import numpy as np
from skill_lvl_key_words import level_score_


def skill_pair_entity2word(input_path):
    # input_path = "kg_nodes_full_data_v2.2_2019_1_10_15_55_22"
    skill_pair = {}
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f.readlines():
            line = json.loads(line)
            name = line.get("name")
            alias = line.get("alias")
            mentions = line.get("mentions")
            if alias:
                for x in alias:
                    skill_pair[x] = name
            if mentions:
                for y in mentions:
                    skill_pair[y] = name
    return skill_pair


skill_entity_dict = skill_pair_entity2word("../data/kg_nodes_full_data_v2.2_2019_1_23_14_1_57.jsonl")


def fun_lvl4_to_lvl3():
    lvl4_to_lvl3 = {}
    with open("../data/fun_lvl4_rel_lvl3.jsonl", "r", encoding="utf-8") as f:
        for line in f.readlines():
            line = json.loads(line)
            lvl4_to_lvl3[str(line[0])] = line[1]
    return lvl4_to_lvl3


def skill_word_to_entity(skill_dict):
    skill_lvl_dict = {}
    for k, v in skill_dict.items():
        skill_en = skill_entity_dict.get(k)
        if skill_en:
            skill_entity = skill_en
        else:
            skill_entity = k
        if skill_entity in skill_lvl_dict.keys():
            skill_lvl_dict[skill_entity].extend(v)
        else:
            skill_lvl_dict[skill_entity] = v
    return skill_lvl_dict


def skill_level_score(input_path, output_path, is_entity=True):
    # input_path = "extract_jd_skill_level_funlvl4_v0x2_2"
    # output_path = "fun_skill_level_score_v0x2_2"
    f2_o = open(output_path, "w", encoding="utf-8")
    fun_lvl4Tolvl3 = fun_lvl4_to_lvl3()
    with open(input_path, "r", encoding="utf-8") as f1_in:
        for i, line in enumerate(f1_in.readlines()):
            skill_score_dict = {}
            skills_ct = {}
            skill_max_tf_score = {}
            line = json.loads(line)
            fun_id = line[0]
            skill_dict = line[2]
            print(i, fun_id)
            if is_entity:
                skill_dict = skill_word_to_entity(skill_dict)
            for k, v in skill_dict.items():
                skill_level = [x for x in v if x[1] > 0]
                skill_lvl_score = [[level_score_(x[0]), x[1]] for x in skill_level]
                skills_ct[k] = sum(np.array(skill_lvl_score)[:, 1])
                if skill_lvl_score:
                    average_weighted = np.round(
                        sum([x[0] * x[1] for x in skill_lvl_score]) / sum(np.array(skill_lvl_score)[:, 1]), 2)
                    skill_score_dict[k] = [average_weighted * 0.5, skill_level]
            # 根据所有的技能词词频计算相应的分数
            if skills_ct:
                skill_max_tf = math.log2(max(skills_ct.values()))
                if skill_max_tf < 0.00001:
                    skill_max_tf_score = {k: 50 for k, v in skills_ct.items()}
                else:
                    skill_max_tf_score = {k: math.log2(v) * 50 / skill_max_tf for k, v in skills_ct.items()}

            skill_score_dict_rt = {k: [np.round(v[0] + skill_max_tf_score.get(k, 0), 2), v[1]] for k, v in
                                   skill_score_dict.items()}
            fun_lvl3 = fun_lvl4Tolvl3.get(str(fun_id))
            if fun_lvl3:
                f2_o.write(json.dumps(
                    {"function_id_lvl3": list(fun_lvl3.items())[0][0],
                     "function_id_lvl4": list(fun_id.items())[0][0],
                     "function_name_lvl3": list(fun_lvl3.items())[0][1],
                     "function_name_lvl4": list(fun_id.items())[0][1],
                     "skill_level": dict(sorted(skill_score_dict_rt.items(), key=lambda x: x[1][0], reverse=True))},
                    ensure_ascii=False) + "\n")
            else:
                f2_o.write(json.dumps(
                    {"function_id_lvl4": list(fun_id.items())[0][0],
                     "function_name_lvl4": list(fun_id.items())[0][1],
                     "skill_level": dict(sorted(skill_score_dict_rt.items(), key=lambda x: x[1][0], reverse=True))},
                    ensure_ascii=False) + "\n")

    f2_o.close()


def skill_level_compact(input_path, output_path):
    """
    提取数据中的部分字段为了更好的查看
    :param input_path: source data
    :param output_path: result of extract fields
    :return:
    """
    f_o = open(output_path, "w", encoding="utf-8")
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f.readlines():
            line = json.loads(line)
            skill_level_tmp = line.get("skill_level")
            line["skill_level"] = {k: v[0] for k, v in skill_level_tmp.items()}
            f_o.write(json.dumps(line, ensure_ascii=False) + "\n")
    f_o.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest='command')

    _parser = subparsers.add_parser('skill_level_score')
    _parser.add_argument('--input_path', required=True)
    _parser.add_argument('--output_path', required=True)
    _parser.add_argument('--is_entity', default=True)

    _parser = subparsers.add_parser('skill_level_compact')
    _parser.add_argument('--input_path', required=True)
    _parser.add_argument('--output_path', required=True)

    args = parser.parse_args()

    if args.command == 'skill_level_score':
        skill_level_score(args.input_path, args.output_path, args.is_entity)
    elif args.command == 'skill_level_compact':
        skill_level_compact(args.input_path, args.output_path)
