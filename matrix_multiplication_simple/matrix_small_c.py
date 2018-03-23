from __future__ import print_function
from pyspark import SparkContext
import sys

def mapper(w):
    arr = w.split(" ")
    return (arr[0],  int(broadcastVectorSmall.value[int(arr[1])-1].split(" ")[1]) * int(arr[2]))

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: matrix_small_c.py vectorfile matrixfile outputfile  ", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="MyTestJob")
    vector_small = sc.textFile(sys.argv[1])
    # Give the vector in a correct order
    broadcastVectorSmall = sc.broadcast(sorted(vector_small.collect(),key=lambda val: int(val.split( )[0])))

    matrix_small = sc.textFile(sys.argv[2])
    matrix_vector_small = matrix_small.map(mapper).reduceByKey(lambda x,y: int(x) + int(y))
    matrix_vector_small.saveAsTextFile(sys.argv[3])
    sc.stop()