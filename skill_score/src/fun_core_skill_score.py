"""

"""
import json

import numpy as np
import scipy.stats as st


def fun_core_skill_percent(input_path1, input_path2):
    """
    根据四级职能得到的核心技能词占技能词总和的百分比
    :param input_path1: 四级职能得到的核心技能词
    :param input_path2: skill words 总表
    :return:
    """
    total_core_skill = []
    with open(input_path1, "r", encoding="utf-8") as f:
        for line in f.readlines():
            line = json.loads(line)
            # function_name_lvl4 = line.get("function_name_lvl4")
            skill_ranked = line.get("skill_ranked")
            total_core_skill.extend([x[0] for x in skill_ranked])

    print("total core skill: {}".format(len(set(total_core_skill))))
    with open(input_path2, "r", encoding="utf-8") as f2:
        skill_valid_v0x6 = [x.replace("\n", "").lower() for x in f2.readlines()]
    print("skill_valid_v0x6: {}".format(len(skill_valid_v0x6)))

    inner = set(total_core_skill) & set(skill_valid_v0x6)
    print("The intersection of total_core_skill and skill_valid_v0x6 :{}".format(len(inner)))
    print("The percentage of total_core_skill and skill_valid_v0x6 : {}".format(
        len(set(total_core_skill)) / len(set(skill_valid_v0x6))))


def fun_core_skill_score(input_path, output_path):
    """
    利用核心技能词的特征值 tfidf * pmi 服从从高斯分布计算核心技能词的得分
    :param input_path: 职能计算的对应核心技能词
    :param output_path: 核心skill计算得分结果
    :return:
    """
    f_o = open(output_path, "w", encoding="utf-8")
    with open(input_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f.readlines()):
            print(i)
            line = json.loads(line)
            # function_name_lvl4 = line.get("function_name_lvl4")
            skill_ranked = line.get("skill_ranked")
            tfidf_pmi = [pow(x[1], 1 / 20) for x in skill_ranked]
            score_mean = np.mean(tfidf_pmi)
            score_std = np.std(tfidf_pmi)
            skill_score = {x[0]: st.norm.cdf(pow(x[1], 1 / 20), score_mean, score_std) * 100 for x in skill_ranked}
            line["skill_score"] = skill_score
            f_o.write(json.dumps(line, ensure_ascii=False) + "\n")
    f_o.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest='command')

    fun_core_skill_percent_parser = subparsers.add_parser('fun_core_skill_percent')
    fun_core_skill_percent_parser.add_argument('--input_path1', required=True)
    fun_core_skill_percent_parser.add_argument('--input_path2', required=True)

    fun_core_skill_score_parser = subparsers.add_parser('fun_core_skill_score')
    fun_core_skill_score_parser.add_argument('--input_path', required=True)
    fun_core_skill_score_parser.add_argument('--output_path', required=True)

    args = parser.parse_args()
    if args.command == 'fun_core_skill_percent':
        fun_core_skill_percent(args.input_path1, args.input_path2)
    elif args.command == 'fun_core_skill_score':
        fun_core_skill_score(args.input_path, args.output_path)
