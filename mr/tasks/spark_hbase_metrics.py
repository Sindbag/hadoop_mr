#!/usr/bin/env python

import argparse
import heapq
import json
import datetime
import time
import re

from collections import defaultdict
from pyspark import SparkConf, SparkContext


LOG_FILE_PREFIX = '/user/abardukov/parsed_logs'
OUTPUT_DIR = '/home/abardukov/metrics'

# parsed log example
# ip \t ts \t page \t referrer \t country
# 195.238.239.146 1511989200      /id92710        -       "Russian Federation"

# profile_hits(profile, day) -> [(hour, count)]
# profile_users(profile, day) -> [(hour, count)]
# user_most_visited_profiles(user, day) -> [profile]
# profile_last_three_liked_users(profile, day) -> [user]


def get_context():
    conf = SparkConf().setAppName("Spark metrics collector")
    sc = SparkContext(conf=conf)
    return sc


def get_hour(timestamp, date):
    d = datetime.datetime.strptime(date, '%Y-%m-%d')
    diff = int(timestamp) - time.mktime(d.timetuple())
    return int(diff // 3600)


def does_like(page):
    pat = re.compile('\/id\d+\?like=1')
    return pat.match(page) is True


def get_id(page):
    return page[1:].split('?')[0].strip()


def get_id_like(page):
    return get_id(page), does_like(page)


def get_prepared_profiles(sc, date):
    filename = '{}/{}'.format(LOG_FILE_PREFIX, date)
    log = sc.textFile(filename)
    pat = re.compile('\/id\d+(\?like=1)?')
    parsed_data = (log
        .map(lambda x: x.split('\t'))
        .filter(lambda x: pat.match(x[2]))
        .map(lambda x: (x[0], x[1], get_hour(x[1], date)) + get_id_like(x[2]))
    )
    return parsed_data


def filter_likes(sc, date, likes=False):
    parsed_data = get_prepared_profiles(sc, date)
    parsed_data.cache()
    while 1:
        likes = yield parsed_data.filter(lambda x: x[-1] is likes)

data_gen = None


def get_profile_stab():
    """
    :return: RDD: (idNNNNN, hour): [...ip]
    """
    parsed = data_gen.send(False)
    # (ip, ts, hour, idNNNNN, likes (false))
    return (parsed
        .groupBy(lambda x: (x[3], x[2]))
        .aggregateByKey([], lambda x, y: x + [p[0] for p in y], lambda x, y: x + y)
    )


def get_profile_hits(stab):
    """
    :return: RDD: (idNNNNN, hour): len([...ip])
    """
    # (idNNNNN, hour): [...ip]
    return (stab
        .mapValues(len)
        .map(lambda x: (x[0][0], (x[0][1], x[1])))
        .collect()
    )


def get_profile_users(stab):
    """
    :return: RDD: (idNNNNN, hour): len(set([...ip]))
    """
    # (idNNNNN, hour): [...ip]
    return (stab
        .mapValues(lambda x: len(set(x)))
        .map(lambda x: (x[0][0], (x[0][1], x[1])))
        .collect()
    )


def get_profile_metrics():
    stab = get_profile_stab()
    hits = get_profile_hits(stab)
    users = get_profile_users(stab)
    return hits, users


def get_user_top():
    """
    :return: RDD: (ip, idNNNNN): len([...ip])
    """
    parsed = data_gen.send(False)
    # (ip, ts, hour, idNNNNN, likes (false))
    return (parsed
        .groupBy(lambda x: (x[0], x[3]))
        .aggregateByKey(0, lambda x, y: x + len(y), lambda x, y: x + y)
        .map(lambda x: x[0] + tuple(x[1:]))
        .groupBy(lambda x: x[0])
        .mapValues(lambda v: [p[1] for p in sorted(v, key=lambda t: (-t[2], t[1]))])
        .collect()
    )


def get_likes_for_date(sc, date):
    filename = '{}/{}'.format(LOG_FILE_PREFIX, date)
    log = sc.textFile(filename)
    pat = re.compile('\/id\d+\?like=1')
    # ip \t ts \t page \t referrer \t country
    pages = (log
        .map(lambda x: (x.strip().split('\t')))
        .filter(lambda x: pat.match(x[2]))
        .map(lambda x: (get_id(x[2]), int(x[1]), x[0]))
        .distinct())
    return pages


def get_likes_strike_count(sc, date, days_range=5):
    pdate = datetime.datetime.strptime(date, '%Y-%m-%d')
    likes = []
    for i in range(days_range):
        likes.append(get_likes_for_date(
            sc, (pdate - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
        ))

    if not(len(likes)):
        return []

    if len(likes) and len(likes) > 1:
        reduced = reduce(lambda x, y: x.join(y), likes[1:], likes[0])
    elif len(likes) == 1:
        reduced = likes[0]

    def heappush(h, el):
        heapq.heappush(h, el[1:])
        return heapq.nlargest(3, h, key=lambda x: x[0])

    def heapmerge(lh, rh):
        h = heapq.merge(lh, rh)
        return heapq.nlargest(3, h, key=lambda x: x[0])

    return (reduced
        .groupBy(lambda x: x[0])
        .aggregateByKey([], heappush, heapmerge)
        .collect()
    )


def collect_profile_metrics(date):
    global data_gen
    sc = get_context()
    data_gen = filter_likes(sc, date)
    next(data_gen)
    h, u = get_profile_metrics()
    save_hours_metric(date, h, 'hits')
    save_hours_metric(date, u, 'users')
    del h
    del u
    top = get_user_top()
    save_top_metric(date, dict(top), 'top')
    del top
    likes = get_likes_strike_count(sc, date, 5)
    save_top_metric(date, dict(likes), 'likes')


def save_top_metric(date, top, name):
    with open('{}/{}-{}'.format(OUTPUT_DIR, date, name), 'w+') as f:
        f.write(json.dumps(top))


def save_hours_metric(date, h, name):
    res = defaultdict(lambda: [0] * 24)
    for k, v in h:
        res[k][v[0]] += v[1]
    with open('{}/{}-{}'.format(OUTPUT_DIR, date, name), 'w+') as f:
        f.write(json.dumps(res))


def save_metrics(date, finals):
    # save metrics to hbase
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="HW 2 Referers and 3 day likes collector")
    parser.add_argument("date", type=str)
    parser.add_argument("--range", type=int, dest='range')

    args = parser.parse_args()
    if args.range:
        start = int(args.date.split('-')[-1])
        for i in range(args.range):
            collect_profile_metrics('{}{:02d}'.format(args.date[:-2], start + i))
    else:
        collect_profile_metrics(args.date)
