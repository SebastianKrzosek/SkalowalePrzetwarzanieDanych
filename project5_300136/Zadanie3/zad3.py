from __future__ import print_function
from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: zad3.py <connection_file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession \
        .builder \
        .appName("Zadanie3") \
        .getOrCreate()

    
    airports = ["WAW","WMI","BZG"]

    connections = spark.read.csv(sys.argv[1], header=True, sep=",").rdd \
        .map(lambda col: (col['Source airport'], col['Destination airport']))

    direct = spark.sparkContext.parallelize(airports) \
        .map(lambda row: (row,0))

    nextDirect = direct.map(lambda x: x)

    for _ in range(0,50):
        temp = connections.join(nextDirect) \
            .map(lambda row: (row[1][0], row[1][1]+1))

        nextDirect = temp.subtractByKey(direct)

        if nextDirect.count() == 0:
            break;

        direct = direct.union(temp) \
            .reduceByKey(lambda val1, val2: min(val1, val2))    

    for row in direct.collect():
        print(row) 