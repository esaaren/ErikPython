from __future__ import print_function

from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.ml.feature import Word2Vec
import numpy as np
from numpy import dot
from numpy.linalg import norm


if __name__ == "__main__":
    sc = SparkContext(appName="SparkWord2Vec")
    # Select Hive data, put into DataFrame
    df = HiveContext(sc).sql("select raw_data from erik.erik_poc")
    # Convert to rdd to split data
    rdd = df.rdd
    rdd = rdd.map(lambda x: x[0].split())
    # Convert back to a DataFrame
    columns = ['text_words']
    df_tmp = rdd.map(lambda x: (x,)).toDF()
    df = df_tmp.toDF(*columns)
    df.show()
    # Get the word2Vec model, vector size can
    print ("Creating word2vec model, vector size 200")
    word2vec = Word2Vec(vectorSize=200, minCount=0, inputCol="text_words", outputCol="result")
    model = word2vec.fit(df)

    # Save model
    print ("Saving word2vec model")
    try:
        model.save(sc,'s3://erik-spark-poc/models')
    except:
        print ("Saving model failed")

    # Do a few cool things, create a global view so anyone can query the vocabulary

    print ("Creating tempView : vectors and querying for the word hello")
    model.getVectors().createOrReplaceTempView("vectors")
    sc.sql("select vector from vectors where word ='hello'").show()
    # Transform each word array for each article into a vector, also add it to a vector
    print ("Transforming the model to get a vector per article")
    result = model.transform(df)
    print ("Placing every vector into a matrix")
    X = []
    for row in result.collect():
        text, vector = row
        print("Text: [%s] => \nVector: %s\n" % (", ".join(text), str(vector)))
        X.append(vector)
    # Numpy array
    Z = np.asarray(X)
    # Show how to find similar words
    print ("Finding synonyms for the word sad")
    synonyms = model.findSynonyms('sad', 5)
    synonyms.show()
    print ("Finding the cosine similarity between the first two articles in the set")
    # Show how to get cosine similarity for two article vectors
    cos_sim = dot(Z[0], Z[1]) / (norm(Z[0]) * norm(Z[1]))
    print ("Cosine Similarity: ", cos_sim)
    # Start
    sc.start()
