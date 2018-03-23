from __future__ import print_function
from pyspark import SparkContext
import sys
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: testjob  ", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="MyIntegerJob")
    dataAll = sc.textFile(sys.argv[1])
    average = dataAll.map(lambda w:(1,w)).reduceByKey(lambda x,y:int(x)+int(y))
    count = dataAll.count()
    average_final = average.map(lambda x: ('average', float(x[1])/count))
    average_final.saveAsTextFile(sys.argv[2])
    sc.stop()
