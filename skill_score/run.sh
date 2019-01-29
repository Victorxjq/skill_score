#!/usr/bin/env bash

DRIVER_PYTHON_BIN="/opt/userhome/kdd_xijunquan/anaconda3/envs/python36/bin/python"

HDFS_PYTHON_PATH="hdfs:///user/kdd_baiyongbin/python/python36.zip"

EXECUTOR_PYTHON_ENV="./pyenv"
EXECUTOR_PYTHON_BIN="$EXECUTOR_PYTHON_ENV/python36/bin/python"


spark-submit \
    --conf spark.pyspark.driver.python=$DRIVER_PYTHON_BIN \
    --conf spark.pyspark.python=$EXECUTOR_PYTHON_BIN \
    --conf spark.yarn.dist.archives=$HDFS_PYTHON_PATH"#"$EXECUTOR_PYTHON_ENV \
    --master yarn \
    --deploy-mode client \
    --executor-memory 1G \
    --executor-cores 1 \
    --num-executors 400 \
    ./read_file_join.py