from pyspark.sql import *
from pyspark.sql import functions as F
from operator import add
import numpy as np
from pyspark import SparkContext
import sys

def common_friends(other):
    other_user = other.split("\t")[0]
    other_friends = other.split("\t")[1].split(",")
    if (other_user == user.value):
        return (user.value,[(0,0)])
    elif(other_user in friends.value):
        return (user.value,[(0,0)])
    else :
        common_counts = len(set(friends.value) & set(other_friends))
        return (user.value,[(int(other_user), common_counts)])
            
def output_mapper(line):
    asd = str(line[0]) + "\t"
    for val in line[1][:10]:
        if val[1]!=0:
            asd += str(val[0])+","
    return asd[:-1]


if __name__ == "__main__":

	if len(sys.argv) != 3:
        print("Usage: recommendation.py inputfile outputfile", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Recommendation")

    friendship_rdd = sc.textFile(sys.argv[1]).repartition(10).cache()

    friendship_list = friendship_rdd.collect();
	test_users = ["8941","9020","9021","9993"]
	recommendation_list = []
	test_users_lines = []
	for line in friendship_list:
	    for test_user in test_users:
	        if(test_user == line.split("\t")[0] ):
	            test_users_lines.append(line)
	            
	for userLine in test_users_lines:
	    friends = sc.broadcast(userLine.split("\t")[1].split(","))
	    user = sc.broadcast(userLine.split("\t")[0])
	    recommendation =  friendship_rdd.map(common_friends).reduceByKey(lambda x,y: sorted(x+y,key=lambda val: (val[1],-val[0]) ,  reverse=True)).map(output_mapper)     
	    recommendation_list.append(recommendation)

	final_rdd = sc.union(recommendation_list)
	final_rdd.repartition(1).saveAsTextFile(sys.argv[2])
	sc.stop()
