#!/usr/bin/env python

import sys

import datetime


def users_per_day():
    curr_user = ''
    signup = False
    date = ''
    first_referer = ''

    for line in sys.stdin:
        ip, date, page, referer, country = map(lambda x: x.strip(), line.split('\t'))

        if ip != curr_user:
            dt = datetime.datetime.fromtimestamp(int(date) * 1.0)
            if curr_user:
                print '\t'.join([curr_user, dt.strftime('%Y%m%d'), first_referer, '1' if signup else '0'])
            curr_user = ip
            first_referer = referer
            signup = False

        signup = signup or page == '/signup'

    dt = datetime.datetime.fromtimestamp(int(date) * 1.0)
    print '\t'.join([curr_user, dt.strftime('%Y%m%d'), first_referer, '1' if signup else '0'])


if __name__ == '__main__':
    users_per_day()