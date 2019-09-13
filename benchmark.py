#!/usr/bin/env python

import subprocess
import sys
from subprocess import PIPE


def extract(filename, table):
    overall_count = 0
    overall_time = 0
    count = 0
    time = 0

    with open(filename, 'r') as f:
        for line in f.readlines():
            for i in range(3):
                if not line.strip():
                    continue

                try:
                    sql = line.format(table)
                    p = subprocess.Popen(["clickhouse-client", "--time", "--format=Null", "-q", sql],
                                stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
                except OSError:
                    break

                out = p.stderr.read()
                if out:
                    time += float(out)
                    count += 1
                    overall_time += float(out)
                    overall_count += 1

                    if count == 100:
                        print("count = ", count, "time = ", time)
                        time = 0
                        count = 0

        print("overall count = ", overall_count, "time = ", overall_time)


if sys.argv[1] == '--help':
    print('benchmark.py <queries_file> <clickhouse_table>')
    exit(0)

extract(sys.argv[1], sys.argv[2])
