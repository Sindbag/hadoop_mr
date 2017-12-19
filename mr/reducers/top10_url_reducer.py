#!/usr/bin/env python

import sys
import heapq


def top_urls():
    heap = []
    curr_page = ''
    count = 0
    for line in sys.stdin:
        page = line.split('\t')[0].strip()
        if page != curr_page:
            heapq.heappush(heap, (-count, curr_page))
            if len(heap) > 10:
                heap = heapq.nsmallest(10, heap)
            curr_page = page
            count = 1
        else:
            count += 1

    heapq.heappush(heap, (-count, curr_page))
    if len(heap) > 10:
        heap = heapq.nsmallest(10, heap)
    for (c, page) in heap:
        print '\t'.join([page, str(-c)])

if __name__ == '__main__':
    top_urls()