import requests
import json

def jaccard_similarity(list1, list2):
    intersection = len(list(set(list1).intersection(list2)))
    # print(list(set(list1).intersection(list2)))
    union = (len(list1) + len(list2)) - intersection
    return float(intersection / union)

def skill_request_ner(text):
    url = "http://192.168.1.210:51655/ner"
    headers = {'Content-type': 'application/json'}
    requestData = {"header": {"log_id": "0x666", "user_ip": "192.168.8.52", "provider": "algo_survey",
                              "product_name": "algo_survey", "uid": "0x666"},
                   "request": {"c": "tagging", "m": "ner", "p": {"query_body": {"text_list": [text]}}}}
    ret = requests.post(url, json=requestData, headers=headers)
    result = json.loads(ret.text)
    return result['response']['results'][0]


def kg_request_query(skill_name):
    url = "http://192.168.1.210:51699/kg"
    headers = {'Content-type': 'application/json'}
    requestData = {
        "header": {
            "log_id": "0x666",
            "user_ip": "192.168.8.52",
            "provider": "algo_survey",
            "product_name": "algo_survey",
            "uid": "0x666"
        },
        "request": {
            "c": "skill",
            "m": "basic_query",
            "p": {
                "query_body": {
                    "query_depth": 1,
                    "skill_name": skill_name
                }
            }
        }
    }
    ret = requests.post(url, json=requestData, headers=headers)
    result = json.loads(ret.text)
    return result['response']['results']

def cacaulate_skill_score(query):
    result = []
    cv_skill_list = []
    if skill_request_ner(query)!=[]:
        for val in skill_request_ner(query):
            cv_skill_list.append(val['text'])
        cv_skill_list = list(set(cv_skill_list))

    if cv_skill_list!=[]:
        for val in cv_skill_list:
            skill_name=val
            skill_score=''
            kg_query = kg_request_query(val)
            related_skill = []
            if kg_query != []:
                for val_kg in kg_query:
                    related_skill.append(val_kg[0])
                    related_skill.append(val_kg[2])
            related_skill = list(set(related_skill))
            if val in related_skill:
                related_skill.remove(val)
            cv_skill_list_tmp = cv_skill_list.copy()
            cv_skill_list_tmp.remove(val)
            # print('related_skill', related_skill)
            # print('cv_skill_list', cv_skill_list)
            if related_skill != [] and cv_skill_list_tmp != []:
                skill_score=jaccard_similarity(cv_skill_list_tmp, related_skill)
                # print('jaccard_similarity', jaccard_similarity(cv_skill_list_tmp, related_skill))
            res={'skill_name':skill_name,'jaccard_similarity':skill_score}
            result.append(res)
    return result

if __name__ == '__main__':
    input_path='./skill_score_test/part-24685'
    output_path='./skill_score_test/result_test'
    output_file=open(output_path,'w')
    with open(input_path) as input_file:
        for line in input_file.readlines():
            line=line.strip('\n')
            skills=cacaulate_skill_score(str(line))
            cv_id=eval(line)[0]
            output_file.write(str(cv_id)+'\t'+str(skills))
            output_file.write('\n')
    output_file.close()

