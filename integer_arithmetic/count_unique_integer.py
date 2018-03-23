from __future__ import print_function
from pyspark import SparkContext
import sys
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: testjob  ", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="MyIntegerJob")
    dataAll = sc.textFile(sys.argv[1])
 #    df = dataAll.map(lambda r: sql.Row(r)).toDF(["line"])

	# uniqueDF = df.dropDuplicates()

	# uniqueDF.count()

	# uniqueDF.write.csv(sys.argv[2])

    unique = dataAll.map(lambda w:(w,0)).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda w: ("distinct",1)).reduceByKey(lambda x,y: int(x)+int(y))
	
    unique.saveAsTextFile(sys.argv[2])
    sc.stop()
