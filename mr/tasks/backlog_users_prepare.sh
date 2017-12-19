#!/usr/bin/env bash

OUTPUT="backlog"
NUM_REDUCERS=8

if [ $# -ge 1 ]; then
    DATE=$1
else
    DATE="2017-10-01"
fi

hdfs dfs -rm -r -skipTrash ${OUTPUT}/${DATE}

if [ ! -d "/home/abardukov/hdfs/parsed_logs/${DATE}/" ]; then
    /home/abardukov/mr/tasks/parse_access_log.sh ${DATE}
fi

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.job.name="Prepare user on day data" \
    -D mapred.text.key.partitioner.options=-k1,1 \
    -D mapred.text.key.comparator.options='-k1,1 -k2,2n -k4,4r' \
    -D mapreduce.job.reduces=$NUM_REDUCERS \
    -files /home/abardukov/mr/mappers/trivial_mapper.py,/home/abardukov/mr/reducers/backlog_users_prepare.py \
    -mapper trivial_mapper.py \
    -reducer backlog_users_prepare.py \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    -input /user/abardukov/parsed_logs/${DATE} \
    -output ${OUTPUT}/${DATE} \
    -jobconf stream.num.map.output.key.fields=4
echo 'backlog data for' ${DATE} 'done'
