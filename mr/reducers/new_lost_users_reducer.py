#!/usr/bin/env python
import argparse
import sys

import datetime


class VisitTypes(object):
    NEW_USER = 1
    LOST_USER = 2
    OTHER = 0


def was_on_date(dates, new_date, lost_date):
    dates.sort()
    filtered_dates = filter(lambda x: lost_date <= x <= new_date, dates)

    if not len(filtered_dates):
        return VisitTypes.OTHER

    if filtered_dates[-1] == lost_date:
        return VisitTypes.LOST_USER

    elif filtered_dates[0] == new_date:
        return VisitTypes.NEW_USER

    return VisitTypes.OTHER


def new_lost_users_count(new_date, lost_date):
    new_fb_users = 0
    converted_today = 0
    new_users = 0
    lost_users = 0
    curr_user = ''
    first_referer = ''

    was_on = []
    signup_on = None

    for line in sys.stdin:
        ip, date, referer, signup = map(lambda x: x.strip(), line.split('\t'))

        if ip != curr_user:
            if curr_user:
                was_type = was_on_date(was_on, new_date, lost_date)

                if was_type == VisitTypes.NEW_USER:
                    new_users += 1
                elif was_type == VisitTypes.LOST_USER:
                    lost_users += 1

                if 'facebook' in first_referer:
                    new_fb_user = False
                    for i in range(3):
                        new_fb_user = new_fb_user or \
                                      was_on_date(
                                          was_on,
                                          new_date - datetime.timedelta(days=i),
                                          lost_date
                                      ) == VisitTypes.NEW_USER
                    new_fb_users += int(new_fb_user)
                    if new_fb_user and signup_on == new_date:
                        converted_today += 1

            curr_user = ip
            first_referer = referer
            was_on = []
            signup_on = None

        parsed_date = datetime.datetime.strptime(date, '%Y%m%d')
        if parsed_date not in was_on:
            was_on.append(parsed_date)

        if int(signup) and (signup_on is None):
            signup_on = parsed_date

    if curr_user:
        was_type = was_on_date(was_on, new_date, lost_date)

        if was_type == VisitTypes.NEW_USER:
            new_users += 1
        elif was_type == VisitTypes.LOST_USER:
            lost_users += 1

        if 'facebook' in first_referer:
            new_fb_user = False
            for i in range(3):
                new_fb_user = new_fb_user or \
                              was_on_date(
                                  was_on,
                                  new_date - datetime.timedelta(days=i),
                                  lost_date
                              ) == VisitTypes.NEW_USER
            new_fb_users += int(new_fb_user)
            if new_fb_user and signup_on == new_date:
                converted_today += 1

    print '\t'.join([str(new_users), str(lost_users),
                     str(new_fb_users),
                     str(converted_today)])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="HW 1 New/Lost Users Counter")
    parser.add_argument("new_date", type=str)

    args = parser.parse_args()
    date = datetime.datetime.strptime(args.new_date, '%Y-%m-%d')
    new_lost_users_count(date, date - datetime.timedelta(days=13))