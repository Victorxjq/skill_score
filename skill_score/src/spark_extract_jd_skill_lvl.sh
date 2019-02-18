#!/bin/bash

DRIVER_PYTHON_BIN="/opt/userhome/kdd_xijunquan/anaconda3/envs/python36/bin/python3.6"

HDFS_PYTHON_PATH="hdfs:///user/kdd_xijunquan/python/python36.zip"

EXECUTOR_PYTHON_ENV="./pyenv"
EXECUTOR_PYTHON_BIN="$EXECUTOR_PYTHON_ENV/python36/bin/python"

INPUT_PATH="/user/kdd_xijunquan/cv_skill_score/icdc/_temporary/0/task_20190215163431_0013_m_024685/part-24685"
OUTPUT_PATH="/user/kdd_xijunquan/extract_jd_skill_level_funlvl4/"

spark-submit \
    --conf spark.pyspark.driver.python=$DRIVER_PYTHON_BIN \
    --conf spark.pyspark.python=$EXECUTOR_PYTHON_BIN \
    --conf spark.yarn.dist.archives=$HDFS_PYTHON_PATH"#"$EXECUTOR_PYTHON_ENV \
    --master yarn \
    --deploy-mode client \
    --executor-memory 1G \
    --executor-cores 1 \
    --num-executors 400 \
    ./extract_jd_skill_lvl.py \
    extract_skill_words_sentence --input_path ${INPUT_PATH} --output_path ${OUTPUT_PATH} \
