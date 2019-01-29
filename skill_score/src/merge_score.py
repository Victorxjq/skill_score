"""
合并利用核心技能词特征计算的score和利用skill熟练度副词计算的score
"""
import argparse
import json

import numpy as np


def merge_skill_score(input_path1, input_path2, output_path):
    """
    合并技能词两次计算得分
    :param input_path1: 利用熟悉程度副助词计算的skill score
    :param input_path2: 利用职能核心技能特征(tf-idf * pmi)计算的skill score
    :param output_path: 合并两种方式计算的skill score
    :return:
    """
    fun_skill_level_score = {}
    with open(input_path1, "r", encoding="utf-8") as f1:
        for line in f1.readlines():
            line = json.loads(line)
            function_id_lvl4 = line.get("function_id_lvl4")
            skill_level = line.get("skill_level")
            skill_level_ = {k: v[0] for k, v in skill_level.items()}
            fun_skill_level_score[function_id_lvl4] = skill_level_
    f_o = open(output_path, "w", encoding="utf-8")
    with open(input_path2, "r", encoding="utf-8") as f2:
        for line in f2.readlines():
            line = json.loads(line)
            skill_score_merge = {}
            function_id_lvl4 = line.get("function_id_lvl4")
            core_skill_score = line.get("skill_score")
            level_skill_score = fun_skill_level_score.get(str(function_id_lvl4))
            del line["skill_score"]
            del line["skill_ranked"]
            if level_skill_score:
                for x in set(core_skill_score.keys()) & set(level_skill_score.keys()):
                    item_score = core_skill_score.get(x)
                    item_level = level_skill_score.get(x)
                    skill_score_merge[x] = np.round((item_score + item_level) / 2, 4)
            line["skill_score_merge"] = dict(sorted(skill_score_merge.items(), key=lambda x: x[1], reverse=True))
            f_o.write(json.dumps(line, ensure_ascii=False) + "\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest='command')

    merge_parser = subparsers.add_parser('merge_skill_score')
    merge_parser.add_argument('--input_path1', required=True)
    merge_parser.add_argument('--input_path2', required=True)
    merge_parser.add_argument('--output_path', required=True)
    args = parser.parse_args()
    if args.command == 'merge_skill_score':
        merge_skill_score(args.input_path1, args.input_path2, args.output_path)
