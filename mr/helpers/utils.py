#!/usr/bin/env python
import re

from datetime import datetime
import time


def splitter(line):
    tokens = line.strip('"').split()
    return tokens[0], tokens[1]


def get_timestamp(line):
    line_parts = line.split()
    dt = line_parts[0]
    date = datetime.strptime(dt, "%d/%b/%Y:%H:%M:%S %z")
    date = time.mktime(date.timetuple())
    return date


def parse_access_log(line):
    pattern = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'
    tmp = re.match(pattern, line).groups()

    method, page = splitter(tmp[2])
    return {
        'ip': tmp[0],
        'date': get_timestamp(tmp[1]),
        'method': tmp[2],
        'page': page,
        'code': tmp[3],
        'content-length': tmp[4],
        'referer': tmp[5],
        'user-agent': tmp[6]
    }
