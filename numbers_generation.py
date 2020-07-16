import random,json,time
import sys

NUM_FILES = int(sys.argv[1])
NUM_COL = int(sys.argv[2])
PRE = sys.argv[3]
NUM_ROWS = int(sys.argv[4])

start_time=time.time()


writers = [open("{}{}.txt".format(PRE, i), "w") for i in range(NUM_FILES)]
count = 0
for row in range(NUM_ROWS):
    for j in range(NUM_FILES):
        data = ','.join([str(random.randint(10000, 1000000)) for i in range(NUM_COL)])
	writers[j].write("{}\n".format(data))
	
print("Total Time (sec): "+str(time.time()-start_time))




