#!/usr/bin/env python

import subprocess
import sys
import multiprocessing

files = [
];

def run(q, line):
    if line == 'end':
        q.put('end')
        return

    sql = 'explain (verbose) ' + line
    try:
        stdout = subprocess.check_output(["psql", "-d", "kpi_shard", "-U", "postgres", "-c", sql])
    except subprocess.CalledProcessError as e:
        print(e.output)
        print(line)
        return
    except Exception as e:
        print("error", str(e))
        return

    pos = stdout.find('Remote SQL')
    pos2 = stdout.find('\n\n')
    if pos != -1:
        pos += 12
        stdout = stdout[pos:pos2]
        q.put('\n'.join(stdout.splitlines()[:-1]))


def listener(filename, q):
    with open('explain/ch_' + filename, 'w') as f:
        while True:
            m = q.get()
            if m == 'end':
                break

            f.write(m + '\n')
            f.flush()


def extract(filename):
    manager = multiprocessing.Manager()
    q = manager.Queue()
    pool = multiprocessing.Pool(multiprocessing.cpu_count() + 2)
    pool.apply_async(listener, (filename, q))

    with open("/tmp/" + filename) as f:
        for line in f.readlines():
            try:
                pool.apply_async(run, (q, line, ))
            except KeyboardInterrupt:
                break

        pool.apply_async(run, (q, 'end', ))

    pool.close()
    pool.join()


for fname in files:
    extract(fname)
