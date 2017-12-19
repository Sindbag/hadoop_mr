#!/usr/bin/env python
import argparse
import json

INPUT_DIR = '/home/abardukov/hdfs/new_lost_users'
OUTPUT_DIR = '/home/abardukov/metrics'


def new_lost_collect(date):
    print date
    sum_new, sum_lost, sum_fb, sum_fb_today = 0, 0, 0, 0
    for part in range(8):
        file = '{}/{}/part-0000{}'.format(INPUT_DIR, date, part)
        with open(file, 'r') as f:
            for line in f:
                new_users, lost_users, fb, fb_today= map(int, line.strip().split('\t'))
                sum_new += new_users
                sum_lost += lost_users
                sum_fb += fb
                sum_fb_today += fb_today
    finals =  {
        'new_users': sum_new,
        'lost_users': sum_lost,
        'facebook_signup_conversion_3': sum_fb_today * 1.0 / sum_fb
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
    parser = argparse.ArgumentParser(description="HW 1 New/Lost Users collector")
    parser.add_argument("date", type=str)
    parser.add_argument("--range", type=int, dest='range')

    args = parser.parse_args()
    if args.range:
        start = int(args.date.split('-')[-1])
        for i in range(args.range):
            new_lost_collect('{}{:02d}'.format(args.date[:-2], start + i))
    else:
        new_lost_collect(args.date)
