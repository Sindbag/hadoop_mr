#!/usr/bin/env python
import argparse
import json

INPUT_DIR = '/home/abardukov/hdfs/top10_urls'
OUTPUT_DIR = '/home/abardukov/metrics'


def collect_urls(date):
    print date
    pages = []
    for part in range(8):
        file = '{}/{}/part-0000{}'.format(INPUT_DIR, date, part)
        with open(file, 'r') as f:
            for line in f:
                page, count = line.strip().split('\t')
                pages.append((page, int(count)))
    pages.sort(key=lambda x: x[1], reverse=True)
    finals =  {
        'top_10_pages': [x[0] for x in pages[:10]]
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
    parser = argparse.ArgumentParser(description="HW 1 Top Pages Collector")
    parser.add_argument("date", type=str)
    parser.add_argument("--range", type=int, dest='range')

    args = parser.parse_args()
    if args.range:
        start = int(args.date.split('-')[-1])
        for i in range(args.range):
            collect_urls('{}{:02d}'.format(args.date[:-2], start + i))
    else:
        collect_urls(args.date)
