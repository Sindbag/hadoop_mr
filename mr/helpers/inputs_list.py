#!/usr/bin/env python
import argparse

import datetime


INPUT_PATH_PREFIX = '/home/abardukov/hdfs/backlog'


def make_input_args(up_to_date, days_count):
    date = datetime.datetime.strptime(up_to_date, '%Y-%m-%d')
    days = [date - datetime.timedelta(days=i) for i in range(days_count - 1, -1, -1)]
    inputs = ['-input {}/{}'.format(INPUT_PATH_PREFIX, d.strftime('%Y-%m-%d')) for d in days]
    print ' '.join(inputs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="HW 1 New/Lost Users Counter")
    parser.add_argument("-up_to_date", type=str, default=(datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d'))
    parser.add_argument("-days_count", type=int, default=13)
    args = parser.parse_args()
    make_input_args(args.up_to_date, args.days_count)