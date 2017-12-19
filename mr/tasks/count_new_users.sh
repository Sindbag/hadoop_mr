#!/usr/bin/env bash

OUTPUT="new_lost_users"
NUM_REDUCERS=8

if [ $# -ge 1 ]; then
    DATE=$1
else
    DATE="2017-10-20"
fi

hdfs dfs -rm -r -skipTrash ${OUTPUT}/${DATE}

if [ ! -d "/home/abardukov/hdfs/backlog/${DATE}/" ]; then
    /home/abardukov/mr/tasks/backlog_users_prepare.sh ${DATE}
fi

INPUTS=$(/home/abardukov/mr/helpers/inputs_list.py -up_to_date ${DATE} -days_count 16)

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.job.name="Count lost and new users" \
    -D mapred.text.key.partitioner.options=-k1,1 \
    -D mapred.text.key.comparator.options='-k1,1 -k2,2n' \
    -D mapreduce.job.reduces=$NUM_REDUCERS \
    -files /home/abardukov/mr/mappers/trivial_mapper.py,/home/abardukov/mr/reducers/new_lost_users_reducer.py \
    -mapper trivial_mapper.py \
    -reducer "new_lost_users_reducer.py ${DATE}" \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    ${INPUTS} \
    -output ${OUTPUT}/${DATE} \
    -jobconf stream.num.map.output.key.fields=2
echo 'count new/lost users for' ${DATE} 'done'

python /home/abardukov/mr/collectors/new_lost_users_collector.py ${DATE}