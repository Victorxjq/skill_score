import requests
import json
import gensim

import pygtrie

with open("./data/skill_valid_v0x9", "r", encoding="utf-8") as f:
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
    rt_list = list(set(rt_list))
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
    rt_list = list(set(rt_list))
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
    rt_list=list(set(rt_list))
    return rt_list

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

def cacaulate_skill_score(query, model_skill,mode_func_skill,func4_name):
    result = []
    cv_skill_list = []
    #ner
    if skill_request_ner(query)!=[]:
        for val in skill_request_ner(query):
            if val['type']=='skill':
                emb_val = '#skill$' + val['text'].replace(' ', '_').lower()
                func_skill_similarity=0.0
                try:
                    func_skill_similarity=mode_func_skill.similarity(emb_val,func4_name)
                except:
                    pass
                if func_skill_similarity>=0.0:
                    cv_skill_list.append(val['text'].replace(' ','_').lower())
        cv_skill_list = list(set(cv_skill_list))

    #skill_words match:

    if cv_skill_list!=[]:
        for val in cv_skill_list:
            skill_name=val
            kg_skill_score=''
            emb_skill_score=''
            kg_query = kg_request_query(val)
            emb_val='#skill$'+val.replace(' ','_').lower()
            embedding_skills=[]
            try:
                embedding_skills=model_skill.most_similar(emb_val, topn=10)
                embedding_skills=[i[0].split('$')[1] for i in embedding_skills]
            except:
                pass
            # print(embedding_skills)
            related_skill = []
            if kg_query != []:
                for val_kg in kg_query:
                    related_skill.append(val_kg[0].replace(' ','_').lower())
                    related_skill.append(val_kg[2].replace(' ','_').lower())
            related_skill = list(set(related_skill))
            if val in related_skill:
                related_skill.remove(val)
            cv_skill_list_tmp = cv_skill_list.copy()
            cv_skill_list_tmp.remove(val)
            if related_skill != [] and cv_skill_list_tmp != []:
                kg_skill_score=jaccard_similarity(cv_skill_list_tmp, related_skill)
            if embedding_skills !=[] and cv_skill_list_tmp != []:
                emb_skill_score=jaccard_similarity(cv_skill_list_tmp, embedding_skills)
            res={'skill_name':skill_name,'kg_similarity':kg_skill_score,'emb_similarity':emb_skill_score}
            if (kg_skill_score!='' and kg_skill_score>0) or (emb_skill_score!='' and emb_skill_score>0):
                result.append(res)
    return result,cv_skill_list

if __name__ == '__main__':
    input_path='./part-24563'
    output_path='./result_test_filter'
    function_id_name = {}
    # model_skill = gensim.models.KeyedVectors.load_word2vec_format('./agg_processed_feature_skill_20190125.txt', binary=False, encoding='utf-8')
    # model_skill_func = gensim.models.KeyedVectors.load_word2vec_format('./agg_processed_feature_skill_func4_20190125.txt',
    #                                                               binary=False, encoding='utf-8')
    with open("./data/function_taxonomy.txt", "r", encoding="utf-8") as f:
        for x in f.readlines():
            x = x.strip('\n').split('\t')
            if x[0].startswith('42'):
                function_id_name[x[0]] = x[1]
    output_file=open(output_path,'w')
    index_all=0
    index_with_skill=0
    test={'work': {'5AB3AEACF2F8B': {'start_time': '2014年08月', 'end_time': '2015年05月', 'so_far': 'N', 'corporation_name': '潍坊承旋康复中心', 'industry_name': '医疗/护理/美容/保健/卫生服务', 'architecture_name': '', 'position_name': '心理医生', 'title_name': '', 'station_name': '', 'reporting_to': '', 'subordinates_count': 0, 'responsibilities': '负责学员的心理辅导工作', 'management_experience': 'N', 'work_type': '医疗/护理/美容/保健/卫生服务', 'basic_salary': 0, 'bonus': 0, 'annual_salary': 0, 'basic_salary_from': 2.001, 'basic_salary_to': 4, 'salary_month': 0, 'annual_salary_from': 0, 'annual_salary_to': 0, 'corporation_desc': '', 'scale': '20-99', 'city': '', 'corporation_type': '', 'reason': '', 'is_oversea': '', 'achievement': '', 'a_p_b': '心理医生 | 2001-4000元/月', 'corporation_id': 0, 'industry_ids': 0, 'id': '5AB3AEACF2F8B', 'is_deleted': 'N', 'created_at': '2015-06-18 17:33:45', 'updated_at': '2015-06-18 17:33:45', 'sort_id': 1}, '5AB3AEACF2FF7': {'start_time': '2014年01月', 'end_time': '2014年07月', 'so_far': 'N', 'corporation_name': '济宁昂立国际教育', 'industry_name': '教育/培训/院校', 'architecture_name': '', 'position_name': '幼教', 'title_name': '', 'station_name': '', 'reporting_to': '', 'subordinates_count': 0, 'responsibilities': '珠心算、自主美劳老师', 'management_experience': 'N', 'work_type': '教育/培训/院校', 'basic_salary': 0, 'bonus': 0, 'annual_salary': 0, 'basic_salary_from': 2.001, 'basic_salary_to': 4, 'salary_month': 0, 'annual_salary_from': 0, 'annual_salary_to': 0, 'corporation_desc': '', 'scale': '20-99', 'city': '', 'corporation_type': '', 'reason': '', 'is_oversea': '', 'achievement': '', 'a_p_b': '幼教 | 2001-4000元/月', 'corporation_id': 0, 'industry_ids': 0, 'id': '5AB3AEACF2FF7', 'is_deleted': 'N', 'created_at': '2015-06-18 17:33:45', 'updated_at': '2015-06-18 17:33:45', 'sort_id': 2}}, 'skill': {}}
    print(get_merge_skill_words(str(test)))
    print(get_longest_skill_words(str(test)))
    print(get_skill_words_back(str(test)))

    # with open(input_path) as input_file:
    #     for line in input_file.readlines():
    #         line = line.strip('\n')
    #         line = eval(line)
    #         # index_all+=1
    #         # if str(line[1][0]['skill'])!='{}':
    #         #     index_with_skill+=1
    #     # print('skill/all:',index_with_skill,index_all,index_with_skill/index_all)
    #         cv_id = line[0]
    #         cv_tag = line[1][1]['cv_tag']
    #         query_str=str(line[1][0]['skill'])
    #         if isinstance(line[1][0]['work'], dict):
    #             for key in line[1][0]['work'].keys():
    #                 query_str=query_str+str(line[1][0]['work'][key]['responsibilities']).replace('\n','')
    #             if len(query_str)>0 and query_str!='{}':
    #                 cv_tag = json.loads(cv_tag)
    #                 if cv_tag!={} and isinstance(cv_tag, dict):
    #                     func4_id=cv_tag[list(cv_tag.keys())[0]]['should']
    #                     func4_name=''
    #                     if func4_id !=[]:
    #                         func4_id=func4_id[0].split(':')[0]
    #                         if func4_id in function_id_name.keys():
    #                             func4_name='#funlvl4$'+function_id_name[func4_id]
    #                             skills_score,skills=cacaulate_skill_score(query_str, model_skill,model_skill_func,func4_name)
    #                             output_file.write(str(cv_id)+'\t'+func4_name+'\t'+str(skills)+'\t'+str(skills_score))
    #                             output_file.write('\n')
    # output_file.close()

