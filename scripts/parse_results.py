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
          "array_name",
          "total_wall_time",
          "total_cpu_time"]

for i in xrange(1,13):
  HEADERS.append("worker" + str(i) + " time")

CSV = []
TESTS = [] # from master_logfile

BREAKDOWN_HEADERS = []
BREAKDOWN_CSV = []

SIZES_HEADERS = []
SIZES_CSV = []

# Regex for parsing log files
RE_START_TEST = re.compile("\[RUN TEST\].*")
RE_END_TEST = re.compile("\[END TEST\] \[(\w*) (\w*)\] (.*) total wall time: \[(.*)\].*\[(.*)\]")
RE_ACK = re.compile(".*Received ack (.*)\[(\w*)\] Time\[(.*) secs\] from worker: (\d+)")
def parse_coord(dirpath, nworkers, row_prefix):
  logfile_path = os.path.join(dirpath, "master_logfile.txt")
  cur_acks = [] # worker, wall time, status
  headers = ['test_name', 'partition', 'array_name', 'total_wall_time', 'total_cpu_time']
  csv_row = row_prefix[:]
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
        test_row = [mend.group(1), mend.group(2), mend.group(3), mend.group(4), mend.group(5)]
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


# REGEX for parsing worker logfile
RE_START_QUERY = re.compile(".*\{Query Start: (.*)\}")
RE_TIMING = re.compile(".*LOG_END \{(.*)\} WALL TIME: \{(.*)\} secs CPU TIME: \{(.*)\} secs\s*")

# test_breakdowns: {test_label: [[headers], [worker1], [worker2], ...]}
def parse_worker(dirpath, worker, nworkers, ts, test_breakdowns):
  logfile = os.path.join(dirpath, "istc{0}_log.txt".format(worker + 1))
  test_num = 0
  cur_breakdowns = [] # dicts {header, walltime, cputime}
  test_label = ""
  with open(logfile) as f:
    for line in f:
      qstart = RE_START_QUERY.match(line)
      if qstart:
        # append breakdowns from previous query
        if test_label != "":
          test_header = ['worker']
          test_row = [worker]
          for breakdown in cur_breakdowns:
            test_header.append(breakdown['header'])
            test_row.append(breakdown['wall_time'])
          if test_label not in test_breakdowns:
            print "test_label", test_label
            test_breakdowns[test_label] = [test_header]

          test_breakdowns[test_label].append(test_row)



        print qstart.group(1), TESTS[test_num]['test_name'], TESTS[test_num]['partition']
        test_label = "n{0} t{1} {2} {3} {4}".format(nworkers, trial,
            TESTS[test_num]['test_name'], TESTS[test_num]['partition'],
            TESTS[test_num]['array_name'])
        test_num += 1
        cur_breakdowns = []

      mtiming = RE_TIMING.match(line)

      # TODO bug in cpu time mtiming.group(3), fix for next batch
      if mtiming:
        #print mtiming.group(1), mtiming.group(2)
        cur_breakdowns.append({'header': mtiming.group(1) + " (secs)",
          'wall_time': mtiming.group(2),
          'cpu_time': mtiming.group(3)})



def parse_dir(dirpath):
  dname = os.path.basename(dirpath)
  num_nodes, trial, ds1, ds2, ts = dname.split('-')
  array = dname.split('-')
  num_nodes = int(array[0][1:])

  return (num_nodes, trial, ds1, ds2, ts)

def print_csv(headers, csv):
  print ','.join([str(x) for x in headers])
  for row in csv:
    assert(len(headers) == len(row))
    print ','.join([str(x) for x in row])
    print


def print_test_breakdowns(test_breakdowns):
  max_ncol = 0
  for k,v in test_breakdowns.iteritems():
    # header row
    if len(v[0]) > max_col:
      max_ncol = len(v[0])

  for k,v in test_breakdowns.iteritems():
    print k
    if len(v) > max_cols:
      max_cols = len(v)
    for row in v:
      print ','.join([str(x) for x in row])

if __name__ == "__main__":
  dirpath = os.path.normpath(sys.argv[1])
  num_nodes, trial, ds1, ds2, ts = parse_dir(dirpath)
  #print "num_nodes: {0}, trial: {1}, dataset1: {2}, dataset2: {3}, timestamp: {4}".format(num_nodes, trial, ds1, ds2, ts)

  row_prefix = [num_nodes, trial, ds1, ds2, ts]

  parse_coord(dirpath, num_nodes - 1, row_prefix)
  test_breakdowns = {}
  for worker in xrange(1, num_nodes):
    parse_worker(dirpath, worker, num_nodes - 1, ts, test_breakdowns)

  #print_csv(HEADERS, CSV)
  print_test_breakdowns(test_breakdowns)

