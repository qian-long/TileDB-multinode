#!/usr/bin/python
import re
import os
import sys
import math
import glob

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
RE_START_TEST = re.compile(".*\[RUN TEST\] \[(.*) (.*)\].*")
RE_END_TEST = re.compile("\[END TEST\] \[(\w*) (\w*)\] (.*) total wall time: \[(.*)\].*\[(.*)\]")
RE_ACK = re.compile(".*Received ack (.*)\[(\w*)\] Time\[(.*) secs\] from worker: (\d+)")
def parse_coord(dirpath, nworkers, row_prefix):
  logfile_path = os.path.join(dirpath, "master_logfile.txt")
  cur_acks = [] # worker, wall time, status
  headers = ['test_name', 'partition', 'array_name', 'total_wall_time', 'total_cpu_time']
  csv_row = row_prefix[:]
  test_row = []
  with open(logfile_path) as f:
    for line in f:

      mstart = RE_START_TEST.match(line)
      if mstart:
        # reset
        cur_acks = []
        csv_row = row_prefix[:]
        test_row = []


        test_row = [mstart.group(1), mstart.group(2), "", "", ""]
        test = dict(zip(headers, test_row))
        TESTS.append(test)
        csv_row.extend(test_row)



      # search for ack
      mack = RE_ACK.match(line)
      if mack:
        if mack.group(1) != "DEFINE_ARRAY_TAG":
          status, time, worker = (mack.group(2), mack.group(3), mack.group(4))
          cur_acks.append([status, time, worker])
          if status == "DONE":
            csv_row.append(time)
          else:
            csv_row.append("ERROR")


      # search for end of test
      mend = RE_END_TEST.match(line)
      if mend:
        array_name, total_wall_time, total_cpu_time = (mend.group(3), mend.group(4), mend.group(5))
        #print "test_row", test_row
        test_row[-1] = total_cpu_time
        test_row[-2] = total_wall_time
        test_row[-3] = array_name

        test = dict(zip(headers, test_row))
        TESTS[-1] = test
        csv_row[len(row_prefix) + 2] = array_name
        csv_row[len(row_prefix) + 3] = total_wall_time
        csv_row[len(row_prefix) + 4] = total_cpu_time
        CSV.append(csv_row)

  # errors with not end tag
  CSV.append(csv_row)

# REGEX for parsing worker logfile
RE_START_QUERY = re.compile(".*\{Query Start: (.*)\}")
RE_TIMING = re.compile(".*LOG_END \{(.*)\} WALL TIME: \{(.*)\} secs CPU TIME: \{(.*)\} secs\s*")

# test_breakdowns: {test_label: [[headers], [worker1], [worker2], ...]}
def parse_worker(dirpath, worker, nworkers, ts, test_breakdowns):

  matches = glob.glob(os.path.join(dirpath, "w{0}_istc*_log.txt".format(worker)))
  logfile = matches[0]
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
            #print "test_label", test_label
            test_breakdowns[test_label] = [test_header]

          test_breakdowns[test_label].append(test_row)


        #print qstart.group(1), TESTS[test_num]['test_name'], TESTS[test_num]['partition']
        test_label = "n{0},t{1},{2},{3},{4}".format(nworkers, trial,
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
    print ','.join([str(x) for x in row] + [""]*(len(headers) - len(row)))

def stddev(lst):
  """returns the standard deviation of lst"""
  mn = sum(lst) / float(len(lst))
  variance = sum([(e-mn)**2 for e in lst])
  return math.sqrt(variance)

def print_test_breakdowns(test_breakdowns):
  for k,v in test_breakdowns.iteritems():
    # prints test label
    print k
    for row in v:
      # print breakdown names and times for each worker
      print ','.join([str(x) for x in row])

    # compute and print min, max, avg, std for each breakdown
    data_cols = zip(*v[1:])
    mins = []
    maxs = []
    avgs = []
    stds = []
    for i in xrange(1, len(data_cols)):
      data = [float(x) for x in data_cols[i]]
      mins.append(min(data))
      maxs.append(max(data))
      avgs.append(sum(data) / float(len(data)))
      stds.append(stddev(data));
    print "min," + ",".join([str(x) for x in mins])
    print "max," + ",".join([str(x) for x in maxs])
    print "avg," + ",".join([str(x) for x in avgs])
    print "std," + ",".join([str(x) for x in stds])
    print # new line between tests


if __name__ == "__main__":
  dirpath = os.path.normpath(sys.argv[1])
  num_nodes, trial, ds1, ds2, ts = parse_dir(dirpath)
  #print "num_nodes: {0}, trial: {1}, dataset1: {2}, dataset2: {3}, timestamp: {4}".format(num_nodes, trial, ds1, ds2, ts)

  row_prefix = [num_nodes, trial, ds1, ds2, ts]
  #print "row_prefix", row_prefix

  parse_coord(dirpath, num_nodes - 1, row_prefix)
  test_breakdowns = {}
  for worker in xrange(1, num_nodes):
    parse_worker(dirpath, worker, num_nodes - 1, ts, test_breakdowns)

  #print_csv(HEADERS, CSV)
  print_test_breakdowns(test_breakdowns)

