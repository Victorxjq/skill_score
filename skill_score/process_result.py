import gensim
import json
# 简历解析
model = gensim.models.KeyedVectors.load_word2vec_format('./embedding/agg_processed_feature_skill_func4_20190125.txt',binary = False,encoding='utf-8')
if __name__ == '__main__':
    with open('./embedding/skill_level_score.jsonl','r') as input_file:
        for line in input_file.readlines():
            line=line.strip('\n')
            line=json.loads(line)
            func4_name='#funlvl4$'+line['function_name_lvl4'].replace(' ','_')
            for skill in list(line['skill_level'].keys()):
                skill_name='#skill$'+skill.replace(' ','_')
                skill_score=line['skill_level'][skill][0]
                skill_sim=model.similarity(func4_name,skill_name)
                print('func4_name/skill_name/similarity/score',func4_name,skill_name,skill_sim,skill_score)

