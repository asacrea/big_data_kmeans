import sys, re
from random import random
from operator import add
from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import SparseVector

if __name__ == "__main__":
    sc = SparkContext(appName="Python")
    sc.setLogLevel("ERROR")
    docsDir = "/user/csanch35/datasets/gutenberg/"
    outputPath = "/user/csanch35/resultado_practica/data_out1"
    print(docsDir, outputPath)
    files = sc.wholeTextFiles(docsDir)
    names = files.keys()
    documents = files.values().map(lambda doc: re.split('\W+', doc))
    hashingTF = HashingTF(1500)
    tf = hashingTF.transform(documents)
    idf = IDF(minDocFreq=2).fit(tf)
    tfidf = idf.transform(tf)
    clusters = KMeans.train(tfidf, 10, maxIterations=450)

    clusterid = clusters.predict(tfidf).collect()
    names = names.collect()
    dictionary = dict(zip(names, clusterid))
    print(dictionary)
   
    d = sc.parallelize(dictionary.items())
    d.saveAsTextFile(outputPath)
    sc.stop()
