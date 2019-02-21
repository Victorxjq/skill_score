import gensim
import json
# 简历解析
model = gensim.models.KeyedVectors.load_word2vec_format('./embedding/agg_processed_feature_skill_func4_20190125.txt',binary = False,encoding='utf-8')
if __name__ == '__main__':
    output_file=open('./embedding/output.txt','w')
    index=0
    with open('./embedding/skill_level_score.jsonl','r') as input_file:
        for line in input_file.readlines():
            line=line.strip('\n')
            line=json.loads(line)
            func4_name='#funlvl4$'+line['function_name_lvl4'].replace(' ','_').lower()
            skill_name_list=[]
            for skill in list(line['skill_level'].keys()):
                skill_name='#skill$'+skill.replace(' ','_').lower()
                skill_score=line['skill_level'][skill][0]
                try:
                    skill_sim=model.similarity(func4_name,skill_name)
                except:
                    skill_sim=0
                if skill_sim>0:
                    skill_name_list.append(skill_name+'$$'+str(skill_score))
                # print('func4_name/skill_name/similarity/score',func4_name,skill_name,skill_sim,skill_score)
            print(index)
            index+=1
            output_file.write(func4_name+'\t'+'\t'.join(skill_name_list))
            output_file.write('\n')
    output_file.close()

