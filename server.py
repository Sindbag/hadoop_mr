#!/usr/bin/env python

import argparse
import datetime
import getpass
import hashlib
import json
import struct

import requests
from flask import Flask, request, abort, jsonify

app = Flask(__name__)
app.secret_key = "my_secret_key"


def iterate_between_dates(start_date, end_date):
    span = end_date - start_date
    for i in xrange(span.days + 1):
        yield start_date + datetime.timedelta(days=i)


@app.route("/")
def index():
    return "OK!"


@app.route("/api/hw1")
def api_hw1():
    start_date = request.args.get("start_date", None)
    end_date = request.args.get("end_date", None)
    if start_date is None or end_date is None:
        abort(400)
    start_date = datetime.datetime(*map(int, start_date.split("-")))
    end_date = datetime.datetime(*map(int, end_date.split("-")))

    result = {}
    for date in iterate_between_dates(start_date, end_date):
        date_string = date.strftime("%Y-%m-%d")
        result[date_string] = {}
        try:
            with open('metrics/' + date_string) as f:
                r = json.loads(f.read())
                print r
                result[date_string] = r
        except:
            print 'No data for ' + 'total_hits ' + date_string

    return jsonify(result)


@app.route("/api/hw3")
def api_hw3():
    # ?start_date=2017-12-17&end_date=2017-12-17&profile_id=id54962&user_ip=193.41.37.193'

    start_date = request.args.get("start_date", None)
    end_date = request.args.get("end_date", None)
    profile_id = request.args.get("profile_id", None)
    user_ip = request.args.get("user_ip", None)

    return requests.get('http://127.0.0.1:27305/api/hw3', params=request.args).content


def login_to_port(login):
    """
    We believe this method works as a perfect hash function
    for all course participants. :)
    """
    hasher = hashlib.new("sha1")
    hasher.update(login)
    values = struct.unpack("IIIII", hasher.digest())
    folder = lambda a, x: a ^ x + 0x9e3779b9 + (a << 6) + (a >> 2)
    return 10000 + reduce(folder, values) % 20000


def main():
    parser = argparse.ArgumentParser(description="HW 1 Example")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=login_to_port(getpass.getuser()))
    parser.add_argument("--debug", action="store_true", dest="debug")
    parser.add_argument("--no-debug", action="store_false", dest="debug")
    parser.set_defaults(debug=False)

    args = parser.parse_args()
    app.run(host=args.host, port=args.port, debug=args.debug)

if __name__ == "__main__":
    main()
