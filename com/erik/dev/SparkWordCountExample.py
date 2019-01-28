from __future__ import print_function
from pyspark import SparkContext
import sys


if __name__ == "__main__":
    print("Hello")
    sc = SparkContext()
    sc.addPyFile("classes.zip")
    from HelperTransformations import HelperTransformations
    text_file = sc.textFile('s3://torstar-datateam-workspace/data/raw/samples/textfile')
    counts = text_file.map(lambda x: HelperTransformations.removeStringSpecialCharacters(x)).flatMap(
        lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    print(counts.take(5))
    counts.saveAsTextFile('s3://torstar-datateam-workspace/data/transformed/samples/textfile_output')
    sc.stop()
