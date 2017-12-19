#!/usr/bin/env python

import sys


def url_mapper():
    for line in sys.stdin:
        _, _, page, _, _ = line.strip().split('\t')
        print '\t'.join([page, '1'])

if __name__ == '__main__':
    url_mapper()
