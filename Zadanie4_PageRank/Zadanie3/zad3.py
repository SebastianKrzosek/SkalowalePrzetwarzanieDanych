from __future__ import print_function
from pyspark.sql import SparkSession
import sys


def matrixMap(line, size):
    x = int(line[0])
    value = float(line[1])
    result = []
    for conn in range(2, len(line)):
        if line[conn] is not None:
            result.append((int(x // size), (int(line[conn]), x, 1.0 / value)))
    return result

def vecMap(line, size):
    x = int(line[0])
    value = float(line[1])
    return (int(x // size), (x, value))

def mapProduct(line):
    vec = line[1][0]
    block = line[1][1]
    if vec[0] == block[1]:
        return (block[0],(vec[1]*block[2]))
    else:
        return (block[0], 0)

def sumReduce(value1, value2):
    return value1 + value2

def pageRank(line, vertex, beta):
    num = line[0]
    value = line[1]
    if num == vertex:
        return (num, value * beta + (1-beta))
    else:
        return (num, value * beta)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: zad3.py <Matrix_dir> <Vector_dir> <Vertex> <Size>",
              file=sys.stderr)
        exit(-1)

    spark = SparkSession \
        .builder \
        .appName("Zadanie3") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    beta = 0.8
    vertex = int(sys.argv[3])
    Size = int(sys.argv[4])

    M = spark.read.csv(sys.argv[1], header=False, sep=";") \
        .rdd \
        .flatMap(lambda line: matrixMap(line, Size))

    V = spark.read.csv(sys.argv[2], header=False, sep=";") \
        .rdd

    for _ in range(50):
        V = V.map(lambda line: vecMap(line, Size)) \
            .join(M).map(mapProduct) \
            .reduceByKey(sumReduce) \
            .map(lambda cell: pageRank(cell, vertex, beta))
            
        print(V.collectAsMap())

    spark.stop()
