#!/bin/bash

DRIVER_PYTHON_BIN="/opt/userhome/kdd_baiyongbin/anaconda3/envs/python36/bin/python"

HDFS_PYTHON_PATH="hdfs:///user/kdd_baiyongbin/python/python36.zip"

EXECUTOR_PYTHON_ENV="./pyenv"
EXECUTOR_PYTHON_BIN="$EXECUTOR_PYTHON_ENV/python36/bin/python"

INPUT_PATH="/user/ichongxiang/data/positions/20180518/dedup_json/"
OUTPUT_PATH="/user/kdd_baiyongbin/extract_jd_skill_level_funlvl4"

hadoop fs -rmr ${OUTPUT_PATH}

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
