from __future__ import print_function
from pyspark import SparkContext
import sys

def mapper(w):
    arr = w.split(" ")
    return (arr[0],  int(broadcastVectorSmall.value[int(arr[1])-1].split(" ")[1]) * int(arr[2]))

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: matrix_small_c.py vectorfile matrixfile outputfile  numberofpartition", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="MyTestJob")
    
    matrix_vector_small= []
    for x in range(int(sys.argv[4])):
        matrix_small = sc.textFile(sys.argv[2] + "/matrix_small"+str(x)+".txt")
        vector_small = sc.textFile(sys.argv[1]+"/vector_small"+str(x)+".txt")
        vector = sc.broadcast(sorted(vector_small.collect(),key=lambda val: int(val.split( )[0])))
        # vector = [1,1]
        rddval = matrix_small.map(lambda w: (w.split(" ")[0],int(vector.value[int(w.split(" ")[1])-1].split(" ")[1]) * int(w.split(" ")[2]))).reduceByKey(lambda x,y: int(x)+int(y))
        matrix_vector_small.append(rddval)

    final_rdd = matrix_vector_small[0]
    for x in range (int(sys.argv[4]) - 1):
        final_rdd = final_rdd.union( matrix_vector_small[x+1])

    final = final_rdd.reduceByKey(lambda x,y: int(x) + int(y))
    final.saveAsTextFile(sys.argv[3])

    sc.stop()