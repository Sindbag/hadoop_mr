#!/usr/bin/env bash

OUTPUT="parsed_logs"
NUM_REDUCERS=0
if [ $# -ge 1 ]; then
    DATE=$1
else
    DATE="2017-10-01"
fi
hdfs dfs -rm -r -skipTrash ${OUTPUT}/${DATE}
echo ${DATE}

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="Parse logs" \
    -D mapreduce.job.reduces=$NUM_REDUCERS \
    -files /home/abardukov/mr/mappers/parse_access_log.py,/home/abardukov/mr/helpers/utils.py,hdfs://hadoop2-10.yandex.ru/user/bigdatashad/dicts/IP2LOCATION-LITE-DB1.CSV \
    -mapper 'parse_access_log.py' \
    -input /user/bigdatashad/logs/${DATE}/access.log.${DATE} \
    -output ${OUTPUT}/${DATE}

echo "base logs parsed for: " ${DATE}