#! /usr/bin/python
import random
import sys

random.seed(0)

BOTTOM = 0
TOP = 1000000
COEF = 1000.0/27.5

def size_to_rows(size): #size in megabytes
    size = size * 1000 # size in kilobytes
    return int(size * COEF)

def gen_row():
    return (random.randint(BOTTOM, TOP), random.randint(BOTTOM, TOP), random.randint(BOTTOM, TOP), random.randint(BOTTOM, TOP))

def row_to_string(row):
    return str(row[0]) + "," +str(row[1]) + "," +str(row[2]) + "," +str(row[3]) + "\n"

def gen_file(myrank, num_node, output_size):
    filename = str(output_size) + "MB.csv"
    num_rows = size_to_rows(output_size)
    with open("Data"+"/"+filename, 'w') as myfile:
        for i in xrange(num_rows*num_node):
            row = gen_row()
            if (i % num_node) == myrank-1:
                myfile.write(row_to_string(row))
    print "Done making file", filename



if __name__ == "__main__":
    gen_file(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
