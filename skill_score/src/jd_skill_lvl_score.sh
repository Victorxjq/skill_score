#!/bin/bash
rt_dir=/opt/userhome/kdd_xijunquan/skill_score/skill_score/skill_score/data/result_score
SKILL_CORE_PATH_source=$1

SKILL_CORE_PATH=${rt_dir}/skill_core_score.jsonl
SKILL_LEVEL_PATH=${rt_dir}/skill_level_score.jsonl
MERGE_SKILL_SCORE=${rt_dir}/merge_skill_score.jsonl

hadoop fs -rm -r /user/kdd_xijunquan/extract_jd_skill_level_funlvl4

sh spark_extract_jd_skill_lvl.sh

hadoop fs -getmerge /user/kdd_xijunquan/extract_jd_skill_level_funlvl4/ ${rt_dir}/extract_jd_skill_level_funlvl4

python3 jd_skill_lvl_score.py skill_level_score --input_path ${rt_dir}/extract_jd_skill_level_funlvl4 --output_path ${SKILL_LEVEL_PATH} --is_entity False
python3 fun_core_skill_score.py fun_core_skill_score --input_path ${SKILL_CORE_PATH_source} --output_path ${SKILL_CORE_PATH}
python3 merge_score.py merge_skill_score --input_path1 ${SKILL_LEVEL_PATH} --input_path2 ${SKILL_CORE_PATH} --output_path ${MERGE_SKILL_SCORE}
