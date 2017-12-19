#!/usr/bin/env python

import argparse
import json
import datetime
import re
import calendar
import getpass
import happybase
import logging
import random
import sys

from pyspark import SparkConf, SparkContext


LOG_FILE_PREFIX = '/user/abardukov/parsed_logs'
OUTPUT_DIR = '/home/abardukov/metrics'


def get_context():
    conf = SparkConf().setAppName("Spark metrics collector")
    sc = SparkContext(conf=conf)
    return sc


def get_refs_counts(sc, date):
    filename = '{}/{}'.format(LOG_FILE_PREFIX, date)
    log = sc.textFile(filename)
    return (log
        .map(lambda x: x.split('\t'))
        .groupBy(lambda x: x[0])
        .mapValues(lambda x:
            reduce(lambda y, z: y + [[z[3], int(z[1])]]
                                if abs(y[-1][1]-int(z[1])) > 30 * 60
                                else y[:-1]+[[y[-1][0], int(z[1])]],
                sorted(x, key=lambda k: int(k[1])), [('x', 0)]
            )[1:])
        .flatMapValues(lambda x: x)
        .map(lambda x: (x[1][0], 1))
        .countByKey()
    )


def get_likes_for_date(sc, date):
    filename = '{}/{}'.format(LOG_FILE_PREFIX, date)
    log = sc.textFile(filename)
    pat = re.compile('\/id\d+\?like=1')
    pages = (log
        .map(lambda x: (x.strip().split('\t')[2], 1))
        .filter(lambda x: pat.match(x[0]))
        .distinct())
    return pages


def get_likes_strike_count(sc, date, days_range):
    pdate = datetime.datetime.strptime(date, '%Y-%m-%d')
    likes = []
    for i in range(days_range):
        likes.append(get_likes_for_date(
            sc, (pdate - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
        ))

    if not(len(likes)):
        return 0

    if len(likes) and len(likes) > 1:
        reduced = reduce(lambda x, y: x.join(y), likes[1:], likes[0])
    elif len(likes) == 1:
        reduced = likes[0]

    return reduced.keys().distinct().count()


def save_metrics(date, finals):
    data = {}
    try:
        file = open('{}/{}'.format(OUTPUT_DIR, date), 'r')
        data = json.loads(file.read())
    except Exception:
        pass

    with open('{}/{}'.format(OUTPUT_DIR, date), 'w+') as f:
        for k in finals.keys():
            data[k] = finals[k]
        f.write(json.dumps(data))

    return finals


def collect_spark_metrics(date):
    sc = get_context()
    referers = get_refs_counts(sc, date)
    print 'Referrers collected for {}'.format(date)

    dates_range = 3
    likes_count = get_likes_strike_count(sc, date, dates_range)
    print 'Likes counted for {} for range of {} days'.format(date, dates_range)

    finals = {
        'session_referers': referers,
        'profile_liked_three_days': likes_count
    }
    save_metrics(date, finals)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="HW 2 Referers and 3 day likes collector")
    parser.add_argument("date", type=str)
    parser.add_argument("--range", type=int, dest='range')

    args = parser.parse_args()
    if args.range:
        start = int(args.date.split('-')[-1])
        for i in range(args.range):
            collect_spark_metrics('{}{:02d}'.format(args.date[:-2], start + i))
    else:
        collect_spark_metrics(args.date)
