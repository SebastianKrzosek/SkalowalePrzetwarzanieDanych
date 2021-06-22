from __future__ import print_function
from pyspark.sql import SparkSession
import sys

def multiply(cell):
    temp_A = [] #przygotowyjemy odpowiednie 
    temp_B = [] #listy pomocnicze
    for value in cell:  
        if value[0] == "A": #i odpowiednio je przypisujemy
            temp_A.append(value) #w zaleznosci czy pochodza z macierzy A
        elif value[0] == "B":
            temp_B.append(value) #czy z macierzy B

    #nastepnie przechodzac obie listy zwracamy odpowiednio wspolrzene i wynik mnozenia
    return [((row[1], col[1]),row[2]*col[2]) for row in temp_A for col in temp_B]
   
def sumReduce(value1, value2):
    return value1 + value2

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: zadanie3.py <A_directory> <B_directory>", file=sys.stderr)
        exit(-1)

    spark = SparkSession \
        .builder \
        .appName("Zadanie3 - Matrix multiplication") \
        .getOrCreate()


    A = spark.sparkContext.textFile(sys.argv[1]) \
        .map(lambda line : line.split(';')) \
        .map(lambda cell: (int(cell[1]), ('A', int(cell[0]), float(cell[2]))))

    #1. Czytanie macierzy rzadkiej A ze std. wejscia
    #2. Podzielenie jej ze wzgledu na ";"
    #3. Generujemy pary klucz-wartosc dla macierzy pierwszej (j,('A', i, a_ij))

    B = spark.sparkContext.textFile(sys.argv[2]) \
        .map(lambda line : line.split(';')) \
        .map(lambda cell: (int(cell[0]), ('B', int(cell[1]), float(cell[2]))))
    #1. Czytanie macierzy rzadkiej B ze std. wejscia
    #2. Podzielenie jej ze wzgledu na ";"
    #3. Tak samo generujemy pary dla macierzy drugiej tylko (j,('B', k, b_jk))

    result = A.union(B) \
        .groupByKey() \
        .flatMap(lambda cell: multiply(cell[1])) \
        .reduceByKey(lambda val1, val2 :sumReduce(val1, val2))\
        .sortByKey()
    #1. Dokonujemy unii macierzy A i B
    #2. Grupujemy po kluczach
    #3. Wykonujemy mnożenie
    #4. Redukcja sumujaca
    #5. Sortowanie dla ulatwienia czytania

    for val in result.collect():
        print(val)