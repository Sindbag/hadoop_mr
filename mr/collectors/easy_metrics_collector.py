#!/usr/bin/env python
import argparse
import json

from collections import defaultdict

INPUT_DIR = '/home/abardukov/hdfs/easy_metrics_reduced'
OUTPUT_DIR = '/home/abardukov/metrics'


def collect_metrics(date):
    print date
    sum_hits, sum_users, sum_sessions, sum_length, sum_duration, sum_empty = 0, 0, 0, 0, 0, 0
    by_country = defaultdict(int)
    for part in range(8):
        file = '{}/{}/part-0000{}'.format(INPUT_DIR, date, part)
        with open(file, 'r') as f:
            hits, users, sessions, length, time, empty = map(int, f.readline().strip().split('\t'))
            sum_hits += hits
            sum_users += users
            sum_sessions += sessions
            sum_length += length
            sum_duration += time
            sum_empty += empty
            for line in f:
                country, count = line.strip().split('\t')
                by_country[country.strip('"')] += int(count)

    finals =  {
        'total_hits': sum_hits,
        'total_users': sum_users,
        'average_session_length': sum_length * 1.0 / sum_sessions,
        'average_session_time': sum_duration * 1.0 / sum_sessions,
        'bounce_rate': sum_empty * 1.0 / sum_sessions,
        'users_by_country': by_country
    }
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="HW 1 Easy Metrics collector")
    parser.add_argument("date", type=str)
    parser.add_argument("--range", type=int, dest='range')

    args = parser.parse_args()
    if args.range:
        start = int(args.date.split('-')[-1])
        for i in range(args.range):
            collect_metrics('{}{:02d}'.format(args.date[:-2], start + i))
    else:
        collect_metrics(args.date)
