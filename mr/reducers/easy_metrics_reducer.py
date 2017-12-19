#!/usr/bin/env python

import sys

from collections import defaultdict


def easy_metrics():
    by_country_count = defaultdict(int)
    curr_user = 'notauser'
    users_count = 0
    hits_count = 0
    sessions_count = 0
    sum_sessions_length = 0L
    sum_sessions_time = 0L
    session_length = 0
    session_start = 0
    empty_sessions = 0
    prev_time = 0

    for line in sys.stdin:
        ip, date, page, referer, country = map(lambda x: x.strip(), line.split('\t'))
        hits_count += 1
        new_session = False

        if ip != curr_user:
            users_count += 1
            new_session = True
            by_country_count[country] += 1
            curr_user = ip

        if abs(int(date) - prev_time) > 30 * 60:
            new_session = True

        if new_session:
            if session_length == 1:
                empty_sessions += 1

            sum_sessions_length += session_length
            if prev_time:
                sum_sessions_time += (prev_time - session_start) if session_length > 1 else 0
            sessions_count += 1
            session_length = 1
            if session_start > prev_time:
                print ip, session_start, prev_time
            session_start = int(date)
        else:
            session_length += 1

        prev_time = int(date)

    # for last session
    if session_length == 1:
        empty_sessions += 1

    sum_sessions_length += session_length
    if prev_time:
        sum_sessions_time += (prev_time - session_start) if session_length > 1 else 0

    print '\t'.join([
        str(hits_count),
        str(users_count),
        str(sessions_count),
        str(sum_sessions_length),
        str(sum_sessions_time),
        str(empty_sessions)
    ])

    for c in by_country_count.keys():
        print '\t'.join([c, str(by_country_count[c])])

if __name__ == '__main__':
    easy_metrics()