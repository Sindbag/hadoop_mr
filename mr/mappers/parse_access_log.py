#!/usr/bin/env python

import sys

import logging

sys.path.append('.')
import re

from datetime import datetime
import time


def get_timestamp(line):
    line_parts = line.split()
    dt = line_parts[0]
    date = datetime.strptime(dt, "%d/%b/%Y:%H:%M:%S")
    date = int(time.mktime(date.timetuple()))
    return date


def parse_access_log(line):
    pattern = '^([\d\.:]+) - - \[(\S+ [^"]+)\] "(\w+) ([^"]+) (HTTP/[\d\.]+)" (\d+) (\d+) "([^"]+)" "([^"]+)"$'

    match = re.match(pattern, line)
    if not match:
        return None

    tmp = re.match(pattern, line).groups()

    try:
        return {
            'ip': tmp[0],
            'date': get_timestamp(tmp[1]),
            'method': tmp[2],
            'page': tmp[3],
            'code': tmp[5],
            'content-length': tmp[6],
            'referer': tmp[7],
            'user-agent': tmp[8]
        }
    except Exception as e:
        logging.error('Error happened while parsing log string: {}', e)
    return None


COUNTRIES_BY_IP_FILE = 'IP2LOCATION-LITE-DB1.CSV'


class IPToCountry:
    def __init__(self, geo_file):
        self.countries = list()
        for line in geo_file:
            l = line.strip().split(',')
            lower, upper, code, name = l[0], l[1], l[2], l[3]
            self.countries.append([int(lower.strip('"')), int(upper.strip('"')), code, name])

        geo_file.close()

    def get_country(self, ip):
        left, right = 0, len(self.countries)
        while left + 1 < right:
            mid = (left + right) / 2
            if self.countries[mid][0] <= ip:
                left = mid
            else:
                right = mid

        return self.countries[left][3]


def get_geography():
    geo_file = open(COUNTRIES_BY_IP_FILE, 'r')
    return IPToCountry(geo_file)


def get_number_from_ip(ip):
    byte_0, byte_1, byte_2, byte_3 = map(int, ip.split("."))
    return byte_0 << 24 | byte_1 << 16 | byte_2 << 8 | byte_3 << 0


def record_from_log():
    geodata = get_geography()

    for line in sys.stdin:
        record = parse_access_log(line)
        if not record or record['code'] != '200':
            continue
        try:
            country = geodata.get_country(get_number_from_ip(record['ip']))
            record['country'] = country or '##'
            fields = ['ip', 'date', 'page', 'referer', 'country']
            print '\t'.join([str(record[f]).strip() or '-' for f in fields])
        except Exception as e:
            logging.error('Found error while parsing log line: {}', e)


if __name__ == '__main__':
    record_from_log()
