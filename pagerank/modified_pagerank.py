from pyspark.sql import *
from pyspark.sql import functions as F
from operator import add
import numpy as np
from pyspark import SparkContext
import sys
from decimal import Decimal

def weight_matrix(tupple):
    num_outlinks = len(tupple[1])
    out = []
    for x in range(total_nodes.value):
        if(x+1 in tupple[1]):
            count =  tupple[1].count(x+1)
            m = round (count * (Decimal(1)/ Decimal(num_outlinks)), 15)
            out.append((x+1, tupple[0], m))
    return out

def matrix_vector_mult(tuple):
        return (tuple[0], round((V.value[tuple[1]-1] * tuple[2]) * 0.8, 15))


if __name__ == "__main__":

	if len(sys.argv) != 4:
        print("Usage: modified_pagerank.py inputfile outputpath", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Pagerank")

    graph_rdd = sc.textFile(sys.argv[1]).repartition(10).cache()

    outlink_rdd = graph_rdd.map(lambda x: (int(x.split("\t")[0]),[int(x.split("\t")[1])])).reduceByKey(lambda x,y: x + y).cache()

    total_nodes = sc.broadcast(outlink_rdd.count())

    M = outlink_rdd.flatMap(weight_matrix).cache()


    # Modified pagerank

    local_v = []

    for x in range(total_nodes.value):
        local_v.append(round(Decimal(1)/ Decimal(total_nodes.value), 15))
        
    V = sc.broadcast(local_v)

    local_e = []
    for x in range(total_nodes.value):
        local_e.append(0.2 * (round(Decimal(1)/ Decimal(total_nodes.value), 15)))
        
    E = sc.broadcast(local_e)

    Beta = sc.broadcast(0.8)
    print(V.value)
    print("Sum is ==", sum(V.value))

    valumatching = False
    iterations = 0
    previousDist = 1.0
    while(valumatching == False):
        iterations += 1
        print(iterations)
        v1 = M.map(matrix_vector_mult).reduceByKey(lambda x,y: x+y)
        v1.cache()
        V1 = sorted(v1.collect(), key=lambda val:val[0])
        V1 = map(lambda x: x[1], V1)
        V2 = [sum(x) for x in zip(V1,E.value)]
        currentDist = np.sum((np.array(V.value) - np.array(V2))**2)
        print("Sum is ==", sum(V2))
        print("currentDist ==", currentDist)
        print("previousDist ==", previousDist)
        if(currentDist > previousDist):
            valumatching = True
            print("number of iterations == %d" %(iterations))
        previousDist = currentDist
        V.destroy()
        V = sc.broadcast(V2)

    pagerank = []
    for x in range(len(V.value)):
        pagerank.append((x+1, V.value[x]))
    output_rdd = sc.parallelize(pagerank).repartition(1).saveAsTextFile(sys.argv[2])

    sc.stop()