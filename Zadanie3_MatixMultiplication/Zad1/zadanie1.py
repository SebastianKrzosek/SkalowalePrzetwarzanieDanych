from __future__ import print_function
from pyspark.sql import SparkSession
import sys

def multiplyMap(line, vec):
    x = line[0]     #odpowiednio przypisuje wspolrzedne
    y = line[1]     #przekazanego wiersza
    value = line[2] #oraz wartosc
    if int(y) in vec:   #nastepnie sprawdzam czy dla y mam odpowiednik w wektorze
        return [(int(x), float(value) * vec[int(y)])] #jesli tak to zwracam pierwsza
    return []                                           #wspolrzedna i wynik mnozenia

def sumReduce(value1, value2):
    return value1 + value2

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: zadanie1.py <matrix_directory>", file=sys.stderr)
        exit(-1)

    spark = SparkSession \
        .builder \
        .appName("Zadanie1 - Matrix and Vector multiplication") \
        .getOrCreate()

    vec = {0: 2, 1: -3, 2:0, 3: 9, 4: 1, 5: 2, 6: 2, 7:0, 8: 1}
        #przyklad wektora zaczerpniety z pdf.

    result = spark.sparkContext.textFile(sys.argv[1]) \
        .map(lambda line: line.split(';')) \
        .flatMap(lambda cell: multiplyMap(cell, vec)) \
        .reduceByKey(lambda val1, val2: sumReduce(val1, val2))\
        .sortByKey()
    #1. Czytanie macierzy rzadkich ze std. wejscia
    #2. Podzielenie ich ze wzgledu na ";"
    #3. Mnożenie elementów
    #4. Redukcja sumująca
    #5. Sortowanie dla ulatwienia czytania

    for val in result.collect():
        print(val)