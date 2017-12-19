#!/usr/bin/env python

import sys


def identity_mapper():
    for line in sys.stdin:
        print line.strip()

if __name__ == '__main__':
    identity_mapper()
