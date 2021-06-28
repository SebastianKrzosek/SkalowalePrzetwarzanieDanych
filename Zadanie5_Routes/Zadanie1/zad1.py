from __future__ import print_function
from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: zad1.py <connection_file> <step_numer>", file=sys.stderr)
        exit(-1)

    spark = SparkSession \
        .builder \
        .appName("Zadanie1") \
        .getOrCreate()

    step = int(sys.argv[2])
    airports = ["YHM","YTO","YKF","YTZ","YYZ", "YKZ"]
    #iata.org/en/publications/directories/code-search/

    connections = spark.read.csv(sys.argv[1], header=True, sep=",").rdd \
        .map(lambda col: (col['Source airport'], col['Destination airport']))
    #1. czytamy plik ze std wejscia i przekazujemy go do rdd
    #2. poprzez mapowanie wyciagamy interesujace nas kolumny
    
    direct = spark.sparkContext.parallelize(airports) \
        .map(lambda row: (row,0))

    for i in range(step):
        if i == 0:  #polaczenia bezposrednie
            temp = connections.join(direct) \
                .map(lambda row: (row[1][0], row[1][1]+1))

            result = direct.union(temp) \
                .reduceByKey(lambda val1, val2: min(val1, val2))

        else:   #polaczenia z przesiadkami
            temp = connections.join(temp.subtractByKey(direct)) \
                .map(lambda row: (row[1][0], row[1][1]+1))

            result = result.union(temp) \
                .reduceByKey(lambda val1, val2: min(val1, val2))

    for res in result.collect():
         print(res)         
         