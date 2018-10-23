from __future__ import print_function

import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark.sql import SQLContext, HiveContext
import re

def removeStringSpecialCharacters(s):
    # Replace special characters with " "
    stripped = re.sub("[^\w\s\-\_]","", s)
    # Change any whitespace to one space
    stripped = re.sub("\s+", " ", stripped)
    # Remove start and end whitespace
    stripped = stripped.strip()
    return stripped

def removeStopWords(s, stopwords):
    word_arr = s.split()
    s = " ".join([word for word in word_arr if not word in stopwords])
    return s

def saveData(rdd):
    print('In save a parquet')
    sqlContext = SQLContext(sc)
    if not rdd.isEmpty():
        # Simple example to save each element being streamed in to a parquet file
        # RePartition this before writing if you want in 1 file
        df = rdd.map(lambda x: (x,)).toDF()
        print('Writing file')
        df.write.parquet('s3a://erik-spark-poc/ouputs', mode='append')
    print('Return save as parquet')
    return rdd

def saveDataHive(rdd):
    print('In insert to Hive')
    sqlContext = HiveContext(sc)
    if not rdd.isEmpty():
        # Simple example to apply a few string operations on each element being streamed in
        stopwords = ['is', 'and', 'or', 'of', 'a', 'i']
        rdd = rdd.map(lambda x: x.lower())
        rdd = rdd.map(lambda x: removeStringSpecialCharacters(x))
        rdd = rdd.map(lambda x: removeStopWords(x, stopwords))
        # Only need the column in our data frame, next iteration add an index or something
        columns = ['raw_data']
        # Temporary dataframe which has no column names
        df_tmp = rdd.map(lambda x: (x,)).toDF()
        # Add column name
        df = df_tmp.toDF(*columns)
        print('Writing to hive')
        # Save to Hive - known issue below doesnt work when table is parquet - create table first then
        # Use insertInto()
        # df.write.format("parquet").mode("Append").saveAsTable("erik.erik_poc")
        df.write.insertInto("erik.erik_poc")
    print('Return insert to Hive')
    return rdd

if __name__ == "__main__":
    if len(sys.argv) != 5:
        sys.exit(-1)
    # Spark context and application name
    sc = SparkContext(appName="SparkStreamingApp")
    ssc = StreamingContext(sc, 1)
    # Get the command line arguments
    appName, streamName, endpointUrl, regionName = sys.argv[1:]
    # This creates a DStream object
    lines = KinesisUtils.createStream(
        ssc, appName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 2)
    # Parse the Json that is passed into Kinesis, simple Json structure is { "fname" : "reddit_comment" }
    parsed = lines.map(lambda x: json.loads(x)['fname'])
    # Print each line out
    parsed.map(lambda x: 'Rec in this line: %s\n' % x).pprint()
    # From the DStream object, take each RDD and pass to a function for processing & storing
    parsed.foreachRDD(lambda x: saveDataHive(x))
    # Stream until terminated
    ssc.start()
    ssc.awaitTermination()

