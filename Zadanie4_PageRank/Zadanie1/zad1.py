from __future__ import print_function
from pyspark.sql import SparkSession
import sys

def prepareVec(dim): #przygotowanie wektora
    cord = []
    values = []
    for i in range(dim):
        cord.append(i)
        values.append(1/dim)
    return dict(zip(cord,values))

def multiplyMap(line, vec): #Mnozenie wykorzystane z projektu 3.
    x = int(line[0])
    y = int(line[1])
    value = float(line[2])
    if y in vec:
        return [(x, value * vec[y])]
    return []

def sumReduce(value1, value2):#Suma rowniez z projektu 3.
    return value1 + value2


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: Zad1.py <Matrix_dir> <dim>", file=sys.stderr)
        exit(-1)

    spark = SparkSession \
        .builder \
        .appName("Zadanie1") \
        .getOrCreate()

    M = spark.read.csv(sys.argv[1], header=False, sep=";").rdd
        #wczytanie macierzy ze std wejscia i wrzucenie jej do rdd
    V = prepareVec(int(sys.argv[2]))
        #przygotowanie wektora
    

    for _ in range(50):
        V = M.flatMap(lambda line: multiplyMap(line, V)) \
            .reduceByKey(sumReduce) \
            .collectAsMap()
        #1. Wykonanie mnozenia macierz * wektor
        #2. Wykonanie sumy
        #3. Przygotowanie wyniku do wyswietlenia
    print(V)

    spark.stop()
