import requests
import json
import gensim


import pygtrie

skill_words=[]
skill_dict_name={}
with open("./kgnodes_full.jsonl", "r", encoding="utf-8") as f:
    for line in f.readlines():
        line=line.strip('\n')
        line=json.loads(line)
        skill_words.append(line['name'].lower())
        if 'alias' in line.keys():
            for val_a in line['alias']:
                skill_words.append(val_a.lower())
                skill_dict_name[val_a.lower()]=line['name'].lower()
        if 'mentions' in line.keys():
            for val_m in line['mentions']:
                skill_words.append(val_m.lower())
                skill_dict_name[val_m.lower()]=line['name'].lower()
skill_words = list(set(skill_words))
t = pygtrie.CharTrie()
for k in skill_words:
    t[k] = k





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
                    pair.append(tool_t)
            else:
                rt_list.append(tmp[0])
            start = end
            end_tmp = end + len(rt_list[-1])
            end -= 1
        else:
            end -= 1
    rt_list=[i for i in rt_list if i not in pair]
    rt_list=[skill_dict_name[i] if i in skill_dict_name.keys() else i for i in rt_list]
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


def cv_request_extract(cv_id):
    url = "http://icdc.rpc/icdc_basic"
    headers = {'Content-type': 'application/json'}
    requestData = {"header": {'uname':'zhanglong','local_ip':'127.0.0.1'},
                   "request": {"w":"icdc_basic", "c":"resumes/logic_resume","m":"get_multi_all","p":{"ids":[cv_id]}}}
    ret = requests.post(url, json=requestData, headers=headers)
    print(ret.text)
    result = json.loads(ret.text)
    return result['response']


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
                    "query_depth": 2,
                    "skill_name": skill_name
                }
            }
        }
    }
    ret = requests.post(url, json=requestData, headers=headers)
    try:
        result = json.loads(ret.text)['response']['results']
    except:
        result=None
    return result

def cacaulate_skill_score(query, model_skill,mode_func_skill,func4_name):
    result = []
    cv_skill_list = []
    # #ner
    # if skill_request_ner(query)!=[]:
    #     for val in skill_request_ner(query):
    #         if val['type']=='skill':
    #             emb_val = '#skill$' + val['text'].replace(' ', '_').lower()
    #             func_skill_similarity=0.0
    #             try:
    #                 func_skill_similarity=mode_func_skill.similarity(emb_val,func4_name)
    #             except:
    #                 pass
    #             if func_skill_similarity>=0.0:
    #                 cv_skill_list.append(val['text'].replace(' ','_').lower())
    #     cv_skill_list = list(set(cv_skill_list))

    #skill_words match:
    skill_l=get_skill_words_back(query)
    if skill_l!=[]:
        for val in skill_l:
            emb_val = '#skill$' + val.replace(' ', '_').lower()
            func_skill_similarity = 0.0
            try:
                func_skill_similarity = mode_func_skill.similarity(emb_val, func4_name)
            except:
                pass
            if func_skill_similarity >= 0.0:
                cv_skill_list.append(val.replace(' ', '_').lower())
        cv_skill_list = list(set(cv_skill_list))
    # print(cv_skill_list)
    cv_skill_output=cv_skill_list.copy()
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
            if kg_query != [] and kg_query is not None:
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
            final_score=''
            if kg_skill_score !='' and emb_skill_score!='':
                final_score=(kg_skill_score+emb_skill_score)/2
            elif kg_skill_score=='' and emb_skill_score!='':
                final_score=emb_skill_score
            elif kg_skill_score !='' and emb_skill_score=='':
                final_score = kg_skill_score
            if final_score!='' and final_score>0:
                cv_skill_output.remove(val)
                res = {'skill_name': skill_name, 'score': pow(final_score,1/8)}
                result.append(res)
    cv_skill_output=[i for i in cv_skill_output if len(i)<=6]
    return sorted(result, key=lambda x: x['score'],reverse=True),cv_skill_output

def cacaulate_skill_score_with_input(input_skill_list, model_skill,mode_func_skill,func4_name):
    result = []
    cv_skill_list = []
    skill_l=input_skill_list
    if skill_l!=[]:
        for val in skill_l:
            emb_val = '#skill$' + val.replace(' ', '_').lower()
            func_skill_similarity = 0.0
            try:
                func_skill_similarity = mode_func_skill.similarity(emb_val, func4_name)
            except:
                pass
            if func_skill_similarity >= 0.0:
                cv_skill_list.append(val.replace(' ', '_').lower())
        cv_skill_list = list(set(cv_skill_list))
    cv_skill_output=cv_skill_list.copy()
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
            if kg_query != [] and kg_query is not None:
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
            final_score=''
            if kg_skill_score !='' and emb_skill_score!='':
                final_score=(kg_skill_score+emb_skill_score)/2
            elif kg_skill_score=='' and emb_skill_score!='':
                final_score=emb_skill_score
            elif kg_skill_score !='' and emb_skill_score=='':
                final_score = kg_skill_score
            if final_score!='' and final_score>0:
                cv_skill_output.remove(val)
                res = {'skill_name': skill_name, 'score': pow(final_score,1/8)}
                result.append(res)
    cv_skill_output=[i for i in cv_skill_output if len(i)<=6]
    return sorted(result, key=lambda x: x['score'],reverse=True),cv_skill_output

if __name__ == '__main__':
    input_path='./data_test'
    func_skill_input_path='merge_skill_score.jsonl'
    output_path='./result_test_withinputskills'
    output_path_processed='./result_test_withinputskills_processed'
    dict_func_skill_score={}
    with open(func_skill_input_path) as input_func_skill:
        for line in input_func_skill.readlines():
            line=line.strip('\n')
            line=json.loads(line)
            dict_func_skill_score[line['function_name_lvl4']]=line['skill_score_merge']

    # print(cv_request_extract('12800032'))
    # function_id_name = {}
    # model_skill = gensim.models.KeyedVectors.load_word2vec_format('./agg_processed_feature_skill_20190125.txt', binary=False, encoding='utf-8')
    # model_skill_func = gensim.models.KeyedVectors.load_word2vec_format('./agg_processed_feature_skill_func4_20190125.txt',
    #                                                               binary=False, encoding='utf-8')
    # with open("./data/function_taxonomy.txt", "r", encoding="utf-8") as f:
    #     for x in f.readlines():
    #         x = x.strip('\n').split('\t')
    #         if x[0].startswith('42'):
    #             function_id_name[x[0]] = x[1]
    # output_file=open(output_path,'w')
    # index_all=0
    # index_withskill=0
    # with open(input_path) as input_file:
    #     for line in input_file.readlines():
    #         index_all+=1
    #         line = line.strip('\n')
    #         line=line.split('\t')
    #         if len(line[0])<10 and len(line[0])>2:
    #             try:
    #                 cv_tag = json.loads(line[1])['cv_tag']
    #             except:
    #                 cv_tag='{}'
    #             cv_id = line[0]
    #             try:
    #                 skill_tag = json.loads(json.loads(line[1])['skill_tag'])['work']
    #             except:
    #                 skill_tag='{}'
    #             extracted_cv_skill_list=[]
    #             if skill_tag!='{}':
    #                 for key in skill_tag.keys():
    #                     title=skill_tag[key]['title']
    #                     desc=skill_tag[key]['desc']
    #                     if title!=[]:
    #                         for val_tit in title:
    #                             for val_val_tit in val_tit['entityIdCandidates']:
    #                                 extracted_cv_skill_list.append(val_val_tit['entityName'].lower())
    #                     if desc!=[]:
    #                         for val_des in desc:
    #                             for val_val_des in val_des['entityIdCandidates']:
    #                                 extracted_cv_skill_list.append(val_val_des['entityName'].lower())
    #             extracted_cv_skill_list=list(set(extracted_cv_skill_list))
    #             try:
    #                 cv_tag = json.loads(cv_tag)
    #             except:
    #                 cv_tag={}
    #             if cv_tag!={} and isinstance(cv_tag, dict):
    #                 func4_id=cv_tag[list(cv_tag.keys())[0]]['should']
    #                 func4_name=''
    #                 if func4_id !=[]:
    #                     func4_id=func4_id[0].split(':')[0]
    #                     if func4_id in function_id_name.keys():
    #                         func4_name='#funlvl4$'+function_id_name[func4_id]
    #                         skills_score,skills=cacaulate_skill_score_with_input(extracted_cv_skill_list, model_skill,model_skill_func,func4_name)
    #                         output_file.write(str(cv_id)+'\t'+func4_name+'\t'+str(skills)+'\t'+str(skills_score))
    #                         if skills!=[] or skills_score!=[]:
    #                             index_withskill+=1
    #                         # print(str(cv_id)+'\t'+func4_name+'\t'+str(skills)+'\t'+str(skills_score))
    #                         output_file.write('\n')
    # print('skill/all:',index_withskill,index_all,index_withskill/index_all)
    # output_file.close()





    #             line = eval(line)
    #             cv_id = line[0]
    #             cv_tag = line[1]['cv_tag']
    #             query_str=str(line[1][0]['skill'])
    #             if isinstance(line[1][0]['work'], dict):
    #                 for key in line[1][0]['work'].keys():
    #                     query_str=query_str+str(line[1][0]['work'][key]['responsibilities']).replace('\n','')
    #                 if len(query_str)>0 and query_str!='{}':
    #                     cv_tag = json.loads(cv_tag)
    #                     if cv_tag!={} and isinstance(cv_tag, dict):
    #                         func4_id=cv_tag[list(cv_tag.keys())[0]]['should']
    #                         func4_name=''
    #                         if func4_id !=[]:
    #                             func4_id=func4_id[0].split(':')[0]
    #                             if func4_id in function_id_name.keys():
    #                                 func4_name='#funlvl4$'+function_id_name[func4_id]
    #                                 skills_score,skills=cacaulate_skill_score(query_str, model_skill,model_skill_func,func4_name)
    #                                 output_file.write(str(cv_id)+'\t'+func4_name+'\t'+str(skills)+'\t'+str(skills_score))
    #                                 output_file.write('\n')
    # output_file.close()



    output_file=open(output_path_processed,'w')
    with open(output_path) as input_files:
        for line in input_files.readlines():
            line=line.strip('\n')
            line=line.split('\t')
            cv_id=line[0]
            func_name=line[1]
            skill_list=[]
            for val3 in eval(line[3]):
                if val3['score']>=0.7:
                    skill_list.append([val3['skill_name'],'精通'])
                elif val3['score']>=0.6:
                    skill_list.append([val3['skill_name'],'熟练'])
                elif val3['score']>=0.5:
                    skill_list.append([val3['skill_name'],'良好'])
                else:
                    skill_list.append([val3['skill_name'], '一般'])
            func_name_key = func_name.strip('#funlvl4$')
            if func_name_key in dict_func_skill_score.keys():
                for val2 in eval(line[2]):
                        if val2 in dict_func_skill_score[func_name_key].keys():
                            new_score = dict_func_skill_score[func_name_key][val2]
                            if new_score >= 70.0:
                                skill_list.append([val2, '精通'])
                            elif new_score >= 60.0:
                                skill_list.append([val2, '熟练'])
                            elif new_score >= 50.0:
                                skill_list.append([val2, '良好'])
                            else:
                                skill_list.append([val2, '一般'])
                        else:
                            skill_list.append([val2, '未知'])
            else:
                for val2 in eval(line[2]):
                    skill_list.append([val2, '未知'])
            output_file.write(cv_id+'\t'+func_name+'\t'+str(skill_list))
            output_file.write('\n')
    #
    output_file.close()

