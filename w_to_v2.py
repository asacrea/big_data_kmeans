import sys, re
from random import random
from operator import add
from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import SparseVector

if len(sys.argv) < 3: # use default
    inputDir = "/user/csanch35/datasets/gutenberg/" 
    outputDir = "/user/csanch35/resultado_practica/data_out1"
else: # use the path provided by the user
    inputDir = sys.argv[1]
    outputDir = sys.argv[2]

if __name__ == "__main__":
    #Configure spark environment
    sc = SparkContext(appName="Python")
    #turn off logs
    sc.setLogLevel("ERROR")
    #Dataset source
    docsDir = inputDir
    outputPath = outputDir

    #print(docsDir, outputPath)

    #Read dataset
    files = sc.wholeTextFiles(docsDir)
    #get names of files
    names = files.keys()
    #split documents by word
    documents = files.values().map(lambda doc: re.split('\W+', doc))
    #Create object that will execute hashing trick and term frequency
    hashingTF = HashingTF()
    #use hash TF object to apply the hash funtion in order to transfom words into
    #their numerical representation, and get the term frequency.
    tf = hashingTF.transform(documents)

    #calculate the importance of the words in the documents
    idf = IDF(minDocFreq=5).fit(tf)
    #Calculate the final importance of the frequency of the words in the documents
    tfidf = idf.transform(tf)
    #exectute K-means algorithm
    clusters = KMeans.train(tfidf, 10, maxIterations=450)
    #get information about the clasification
    clusterid = clusters.predict(tfidf).collect()
    #get name of the documents
    names = names.collect()
    #create a dictionary with name of the documents and cluster clasification
    dictionary = dict(zip(names, clusterid))
    print(dictionary)

    #save output
    d = sc.parallelize(dictionary.items())
    d.saveAsTextFile(outputPath)
    sc.stop()
