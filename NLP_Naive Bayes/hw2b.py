from __future__ import print_function

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import NGram
from pyspark.ml.feature import Word2Vec
from pyspark.sql.functions import udf
from pyspark.sql.types import *

from pyspark.ml import Pipeline
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.classification import NaiveBayes

# from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.classification import SVMWithSGD
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.sql.functions import col
from pyspark.mllib.linalg import Vector as MLLibVector, Vectors as MLLibVectors
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LinearRegressionWithSGD, LinearRegressionModel

def numeric_label(value):
	return dictionary[value]

def evaluate(predicted, actual):
    if predicted == actual:
        return 1
    else :
        return 0

if __name__ == "__main__":
	spark = SparkSession.builder.appName("assignment2b").getOrCreate()
	
	train_df = spark.read.json("/user/parth/train.json").cache()
	test_df = spark.read.json("/user/parth/test.json").cache()	

	# Text Labels to numeric labels
	labels = train_df.select('label').distinct()
	a = labels.collect()
	dictionary = {}

	for x in range(len(a)):
    	dictionary[a[x]['label'].encode('ascii','ignore')] = x

	    
	udfNumericLabel = udf(numeric_label, IntegerType())

	train_df = train_df.withColumn("numeric_label", udfNumericLabel("label"))
	test_df = test_df.withColumn("numeric_label", udfNumericLabel("label"))
	train_df = train_df.withColumnRenamed('label','text_label')
	train_df = train_df.withColumnRenamed('numeric_label','label')
	test_df = test_df.withColumnRenamed('label','text_label')
	test_df = test_df.withColumnRenamed('numeric_label','label')
	

	# Tockenize the doc into words
	tokenizer = Tokenizer(inputCol="doc",outputCol="words")
	wordsDataTrain = tokenizer.transform(train_df)
	wordsDataTest = tokenizer.transform(test_df)

	# Remove Stop Words
	remover = StopWordsRemover(inputCol="words",outputCol="filtered")
	sw =remover.getStopWords()
	sw.append('subject')
	sw.append('re')
	sw.append('email')
	remover.setStopWords(sw)
	cleanDataTrain = remover.transform(wordsDataTrain)
	cleanDataTest = remover.transform(wordsDataTest)

	# Made onegrams
	onegram = NGram(n=1, inputCol="filtered",outputCol="onegram")
	onegramedDataTrain=onegram.transform(cleanDataTrain)
	onegramedDataTest = onegram.transform(cleanDataTest)

	#  Find hashed Term frequency value of word vector
	hashingTF = HashingTF(inputCol="onegram", outputCol="rawFeatures", numFeatures=100000)
	featurizedDataTrain = hashingTF.transform(onegramedDataTrain)
	featurizedDataTest = hashingTF.transform(onegramedDataTest)

	# Find IDF
	idf = IDF(inputCol="rawFeatures", outputCol="features")
	idfModelTrain = idf.fit(featurizedDataTrain)
	idfModelTest = idf.fit(featurizedDataTest)
	rescaledDataTrain = idfModelTrain.transform(featurizedDataTrain)
	rescaledDataTest = idfModelTest.transform(featurizedDataTest)

	# Final test and train data
	train_data = rescaledDataTrain.select('features', 'label')
	test_data = rescaledDataTest.select('features', 'label')

	# Multinomial naivebyes
	nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
	model = nb.fit(train_data)
	result = model.transform(test_data)
	result.show()

	# Find accuracy 
	udfEvaluate = udf(evaluate, IntegerType())
	final = result.withColumn("correct", udfEvaluate("prediction", "label")) 
	ac_df = final.groupBy("correct").count()
	ac = ac_df.collect()
	accuracy = float(float(ac[0]['count']) /float(ac[1]['count'] +ac[0]['count']))
	print('Accuracy of the model == ', accuracy)
	ac_df.show()
		
	spark.stop()
	
