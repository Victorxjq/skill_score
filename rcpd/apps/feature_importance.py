# -*- coding=utf-8 -*-
import warnings
import json
from flask import request
from flask import jsonify
from flask_restful import Resource
import numpy as np
from sklearn.tree import DecisionTreeClassifier

warnings.filterwarnings('ignore')


def feature_process(data_input):
    personality_names = ['personality#' + str(i + 1) for i in range(0, 21)]
    quality_names = ['quality#' + str(i + 1) for i in range(0, 27)]
    potential_names = ['potential#' + str(i + 1) for i in range(0, 4)]
    skills = ['professional_skills#' + v for v in data_input['skill_ids']]
    skill_cnt = 0
    dict_skills = {}
    for skill_val in skills:
        dict_skills[skill_val] = skill_cnt
        skill_cnt += 1
    feature_names = personality_names + quality_names + potential_names + skills
    features = []
    target = []
    if isinstance(data_input['data'],str):
        data_feature = eval(data_input['data'])
    else:
        data_feature=data_input['data']
    for val in data_feature:
        # val = json.loads(val)
        # 处理labels
        target.append(val['standard_level'])
        # 处理性格相关维度
        personality_feature = [0] * 21
        personality_input = val['personality_eval']
        if isinstance(personality_input, str):
            personality_input = json.loads(personality_input)
        for k_person in personality_input.keys():
            personality_feature[int(k_person) - 1] = int(personality_input[k_person])
        # 处理素质相关维度
        quality_feature = [0] * 27
        quality_input = val['quality']
        if isinstance(quality_input, str):
            quality_input = json.loads(quality_input)
        for k_quality in quality_input.keys():
            quality_feature[int(k_quality) - 1] = int(quality_input[k_quality])
        # 处理潜力相关维度
        potential_feature = [0] * 4
        potential_input = val['potential']
        if isinstance(potential_input, str):
            potential_input = json.loads(potential_input)
        for k_potential in potential_input.keys():
            potential_feature[int(k_potential) - 1] = int(potential_input[k_potential])
        # 处理知识技能相关维度
        professional_feature = [0] * len(skills)
        professional_input = val['professional_skills']
        if isinstance(professional_input, str):
            professional_input = json.loads(professional_input)
        for k_professional in professional_input.keys():
            skill_encode = dict_skills['professional_skills#'+k_professional]
            professional_feature[skill_encode] = int(professional_input[k_professional])
        feature_unit = personality_feature + quality_feature + potential_feature + professional_feature
        features.append(feature_unit)
    features = np.asarray(features)
    target = np.asarray(target)
    feature_names = np.asarray(feature_names)
    return features, target, feature_names


class Feature_importance(Resource):
    def __init__(self, **kwargs):
        self.header = {
            "appid": 20,
            "log_id": "dev",
            "uid": "",
            "uname": "",
            "provider": "chatbot",
            "signid": "",
            "version": "",
            "ip": ""
        }
        self.response = {
            "err_no": 0,
            "err_msg": "",
            "results": {
            }
        }
        self.logger = kwargs['logger']

    def post(self):
        try:
            data = request.data.decode('utf-8', 'ignore')
            args = json.loads(data)
            data = args.get('p')
            x, y, feature_names = feature_process(data)
            clf = DecisionTreeClassifier(criterion='entropy')
            # 可选gini、entropy
            clf.fit(x, y)
            # feature_importance = clf.feature_importances_
            feature_importance = clf.tree_.compute_feature_importances(normalize=True)
            res = [*zip(feature_names, feature_importance)]
            output = []
            for res_val in res:
                res_name = res_val[0].split('#')
                output_unit = {"axis": res_name[0],
                               "label": res_name[1],
                               "weight": res_val[1]}
                output.append(output_unit)
            self.logger.info("algorithm results: {}".format(output))
            self.response['results'] = {'features': output}
            return jsonify(self.response)
        except Exception as e:
            self.logger.warning("Error happen the segment logic:{}".format(e))
            self.response['err_no'] = 10
            self.response['err_msg'] = e
            self.response['results'] = []
            return jsonify(self.response)
