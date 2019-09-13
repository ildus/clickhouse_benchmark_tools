#!/usr/bin/env python

import subprocess
import sys
import multiprocessing


def run(q, line, table):
    if line == 'kill':
        q.put('kill')
        return

    try:
        sql = line.format(table)
        stdout = subprocess.check_output(["clickhouse-client", "-d", "shard4", "-q", sql])
    except OSError:
        return

    parts = stdout.splitlines()
    if parts:
        q.put(line)


def listener(q):
    with open("ch_working.out", 'w') as f:
        while 1:
            m = q.get()
            if m == 'kill':
                break

            f.write(m + '\n')
            f.flush()


def extract(filename, table):
    manager = multiprocessing.Manager()
    q = manager.Queue()
    pool = multiprocessing.Pool(multiprocessing.cpu_count() + 2)
    watcher = pool.apply_async(listener, (q,))

    with open(filename) as f:
        for line in f.readlines():
            try:
                pool.apply_async(run, (q, line, table))
            except KeyboardInterrupt:
                break

        pool.apply_async(run, (q, "kill", table))

    pool.close()
    pool.join()


if sys.argv[1] == '--help':
    print('create ch_working.out file with queries that responded with some data')
    print('make_working.py <queries_file> <clickhouse_table>')
    exit(0)

extract(sys.argv[1], sys.argv[2])
