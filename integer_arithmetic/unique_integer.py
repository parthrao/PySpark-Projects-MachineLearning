from __future__ import print_function
from pyspark import SparkContext
import sys
from pyspark.sql import SQLContext, Row

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: testjob  ", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="MyIntegerJob")
    sqlContext = SQLContext(sc)
    dataAll = sc.textFile(sys.argv[1])
    # largest = dataAll.map(lambda w:(1,w)).reduceByKey(lambda x,y:max(int(x),int(y)))
    df = dataAll.map(lambda r: Row(r)).toDF(["line"])

    uniqueDF = df.dropDuplicates()

	# uniqueDF.collect()

    uniqueDF.write.csv(sys.argv[2])
	# largest.saveAsTextFile(sys.argv[2])
    sc.stop()
