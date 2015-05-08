#!/usr/bin/python
import re
import os
import sys

HEADERS = ["num_nodes", 
          "trial", 
          "ds1", 
          "ds2", 
          "timestamp",
          "test_name",
          "partition",
          "total_wall_time",
          "total_cpu_time"]

for i in xrange(1,13):
  HEADERS.append("worker" + str(i) + " time")

CSV = []
TESTS = []


RE_START_TEST = re.compile("\[RUN TEST\].*")
RE_END_TEST = re.compile("\[END TEST\] \[(\w*) (\w*)\].*\[(.*)\].*\[(.*)\]")
RE_ACK = re.compile(".*Received ack (.*)\[(\w*)\] Time\[(.*) secs\] from worker: (\d+)")
def parse_coord(dirpath, nworkers, row_prefix):
  logfile_path = os.path.join(dirpath, "master_logfile.txt")
  cur_acks = [] # worker, wall time, status
  headers = ['test_name', 'partition', 'total_wall_time', 'total_cpu_time']
  #HEADERS.extend(headers)
  csv_row = row_prefix[:]
  started_test = False
  with open(logfile_path) as f:
    for line in f:

      # search for ack
      mack = RE_ACK.match(line)
      if mack:
        if mack.group(1) != "DEFINE_ARRAY_TAG":
          cur_acks.append([mack.group(2), mack.group(3), mack.group(4)])

      # search for end of test
      mend = RE_END_TEST.match(line)
      if mend:
        test_row = [mend.group(1), mend.group(2), mend.group(3), mend.group(4)]
        test = dict(zip(headers, test_row))
        TESTS.append(test)
        #print test['test_name']
        csv_row.extend(test_row)
        assert(len(cur_acks) == nworkers)

        for ack in cur_acks:
          status = ack[0]
          time = ack[1]
          worker = ack[2]
          if status == "DONE":
            headers.append('worker' + str(worker))
            csv_row.append(time)
          else:
            csv_row.append("ERROR")

        for i in xrange(len(cur_acks), nworkers):
          csv_row.append("n/a")
        #print "csv_row", csv_row
        CSV.append(csv_row)

        # reset
        cur_acks = []
        csv_row = row_prefix[:]


def parse_worker(logfile):
  pass

def parse_dir(dirpath):
  dname = os.path.basename(dirpath)
  num_nodes, trial, ds1, ds2, ts = dname.split('-')
  array = dname.split('-')
  num_nodes = int(array[0][1:])

  return (num_nodes, trial, ds1, ds2, ts)

def print_csv():
  print ','.join([str(x) for x in HEADERS])
  for row in CSV:
    assert(len(HEADERS) == len(row))
    print ','.join([str(x) for x in row])


if __name__ == "__main__":
  dirpath = os.path.normpath(sys.argv[1])
  num_nodes, trial, ds1, ds2, ts = parse_dir(dirpath)
  #print "num_nodes: {0}, trial: {1}, dataset1: {2}, dataset2: {3}, timestamp: {4}".format(num_nodes, trial, ds1, ds2, ts)

  row_prefix = [num_nodes, trial, ds1, ds2, ts]

  parse_coord(dirpath, num_nodes - 1, row_prefix)
  print_csv()
