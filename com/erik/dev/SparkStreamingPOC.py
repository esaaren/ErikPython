from __future__ import print_function

import sys

import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark.sql.functions import from_json
import re
from pyspark.mllib.feature import Word2Vec


if __name__ == "__main__":
    if len(sys.argv) != 5:
        sys.exit(-1)

    sc = SparkContext(appName="SparkStreamingApp")
    ssc = StreamingContext(sc, 1)
    appName, streamName, endpointUrl, regionName = sys.argv[1:]


    lines = KinesisUtils.createStream(
        ssc, appName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 2)

    parsed = lines.map(lambda x: json.loads(x)['fname'])
    parsed.pprint()

    ssc.start()
    ssc.awaitTermination()