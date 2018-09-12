from __future__ import print_function

import sys

import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark.sql import SQLContext

def saveData(rdd):
    print('In save a parquet')
    sqlContext = SQLContext(sc)
    if not rdd.isEmpty():
        df = rdd.map(lambda x: (x,)).toDF()
        print('Writing file')
        df.write.parquet('s3a://erik-spark-poc/ouputs', mode='append')
    print('Return save as parquet')
    return rdd


if __name__ == "__main__":
    if len(sys.argv) != 5:
        sys.exit(-1)

    sc = SparkContext(appName="SparkStreamingApp")
    ssc = StreamingContext(sc, 1)
    appName, streamName, endpointUrl, regionName = sys.argv[1:]


    lines = KinesisUtils.createStream(
        ssc, appName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 2)

    parsed = lines.map(lambda x: json.loads(x)['fname'])

    parsed.map(lambda x: 'Rec in this line: %s\n' % x).pprint()

    parsed.foreachRDD(lambda x: saveData(x))

    ssc.start()
    ssc.awaitTermination()

