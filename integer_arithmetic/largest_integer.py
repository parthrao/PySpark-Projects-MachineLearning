from __future__ import print_function
from pyspark import SparkContext
import sys
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: testjob  ", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="MyIntegerJob")
    dataAll = sc.textFile(sys.argv[1])
    largest = dataAll.map(lambda w:("largest",w)).reduceByKey(lambda x,y:max(int(x),int(y)))
    largest.saveAsTextFile(sys.argv[2])
    sc.stop()
