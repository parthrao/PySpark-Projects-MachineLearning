from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.sparkSession import SparkSession
from operator import add
import numpy as np
from pyspark import SparkContext
import sys
from decimal import Decimal
import random
from pyspark.sql.functions import monotonically_increasing_id,udf,col
from pyspark.sql.types import LongType,ArrayType,IntegerType,StringType

def get_words(text):
    return text.strip().lower().split( )

def binarizer(l):
    b = []
    for x in universal_set.value:
        if x in l:
            b.append(1)
        else:
            b.append(0)
    return b

def hashing(arr):
    hashed = []
    for ind in range(len(indexMatBroad.value)):
        found = False
        hashValue = 0
        m = 0
        while(found == False):
            j = indexMatBroad.value[ind].index(m)
            
            if(arr[j] == 1):
                found = True
                hashValue = m
                break;
            else:
                m += 1
            
        hashed.append(hashValue)
        
    return hashed

def find_average(x):
    c= np.array([float(0) for l in range(100)]);
    count = 0
    for a in x:
        c += a
        count += 1
    return c/count
    
def find_sse(x):
    cluster_num = x[0]
    # return cluster_num
    sse = 0.0
    
    for a in x[1]:
        sse += np.sum( (meansBroadCast.value[cluster_num][1] - a) ** 2)
    return (cluster_num, sse)
    
def closestPoint(p):
    bestIndex = 0
    closest = 10000000
    for i in range(len(kPoints.value)):
        tempDist = np.sum((p - kPoints.value[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex

if __name__ == "__main__":

	if len(sys.argv) != 4:
        print("Usage: kmeans.py inputpath outputpath", file=sys.stderr)
        exit(-1)
    # sc = SparkContext(appName="K-means")
    spark = SparkSession.builder.appName("K-means").getOrCreate()

    articles_df = spark.read.csv("/user/phr240/bigml/Articles_final.csv", header=True, inferSchema = True,ignoreTrailingWhiteSpace= True)

    # assigning each row a fix id to keep track of it later
    articles_df = articles_df.withColumn("id", monotonically_increasing_id()).cache()

    # udf functoin ro run on each element of a given column -in our case the article
    udf_get_words = udf(get_words, ArrayType(StringType()))

    # Convert article column in bag of words
    words_df =  articles_df.withColumn("words", udf_get_words(col("Article"))).select('id','words').cache()

    # find the universal set of words which will be used to generate binary values for every article
    uni = [] 
    temp2 = words_df.select('words').collect()
    for r in temp2:
        uni += (r.words)
        
    universal_set = spark.sparkContext.broadcast(set(uni))
    universal_set_length = len(universal_set.value)


    # UDF binarizer functoin which converts bag of word column into binary representation
    udf_binarizer = udf(binarizer, ArrayType(IntegerType()))

    # Applying udf_binarizer to words column
    binary_df = words_df.withColumn('binary', udf_binarizer(col('words'))).select('id', 'binary').cache()

    # For better parallelization
    binary = binary_df.repartition(100).cache()

    # IndexMatrix with reshuffled indexArr values which will be used to generate random shuffle of binarySignature to find minhash
    indexArr = [x for x in range(universal_set_length)]
    indexMat = {}
    for i in range(100):
        random.shuffle(indexArr)
        indexMat[i] = list(indexArr)

    indexMatBroad = spark.sparkContext.broadcast(indexMat)


    # MinHashing
    udf_hashing = udf(hashing, ArrayType(IntegerType()))
    hash_df = binary.withColumn('hash', udf_hashing(col('binary'))).select('id', 'hash').cache()

    # Storing this hashed dataframe for safety purpose
    hash_df.write.save("/user/phr240/output/min_hashed", format='parquet')


    # K-mean algorithm
    
    # Convert into rdd with just hashed points of type np.ndarray
    data = hash_df.rdd.map(lambda a:  np.array([float(x) for x in a.hash])).cache()
    max_iterations = 100
    iterations = 0
    tempDist = 100001
    sse = {}
    k=sys.argv[3]

    kPoints = spark.sparkContext.broadcast(data.takeSample(False, k, 1))

    while (tempDist > 10) & (iterations < max_iterations):
        iterations += 1

        # Finding closes centers for every point. (cluster number , point)
        closest = data.map(lambda p: (closestPoint(p), p))

        # Find new centers
        means = sorted(closest.groupByKey().mapValues(find_average).collect(),key=lambda x:  x[0])
        
        # Distance between old and new centers
        tempDist = sum(np.sum((kPoints.value[iK] - p) ** 2) for (iK, p) in means)
        
        kPoints.destroy()
        kPoints = spark.sparkContext.broadcast([p for (i,p) in means])

    meansBroadCast = spark.sparkContext.broadcast(means)

    cstate = closest.groupByKey().map(find_sse).collect()
    final_sse = 0
    for x in cstate:
        final_sse += x[1]

    output_rdd = spark.sparkContext.parallelize([('centers', k), ('sse', final_sse), ('iterations', iterations)], 1)
    output_rdd.saveAsTextFile(sys.argv[2]+str(k))

    
    spark.stop()




















