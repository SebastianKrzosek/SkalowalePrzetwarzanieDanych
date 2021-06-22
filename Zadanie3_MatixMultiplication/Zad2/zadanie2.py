from __future__ import print_function
from pyspark.sql import SparkSession
import math
import sys

def multiplyMap(cell, vect_num):
    x = cell[0]     #Jak poprzednio przypisuje
    y = cell[1]     #wspolrzedne komorki
    value = cell[2] #wartosc
    vector = dict() #oraz deklaruje nowy slownik na wektor
    count = math.floor(int(y) / size) #zmienna pomocnicza sluzaca
    if int(count) != vect_num:   #do otworzenia odpowiedniego pliku wektorow
        vect_num = int(count)           
        f = open(vector_directory + 'vector' + str(vect_num) + '.txt', 'r')\
            .readlines() #otwieramy odpowiedni plik z wektorem
        for line in f:
            temp = line.split(';')  #dzielimy plik
            vector[int(temp[0])] = float(temp[1]) 
                                    #i wstawiamy do wektora
                                    #gdzie temp[0] to wspolrzedna
                                    #zas temp[1] to wartosc

    if int(y) in vector: #nastepnie sprawdzam dla czy y mam odpowiednik w wektorze
        return [(int(x), float(value) * vector[int(y)])] #jesli tak, to zwracam pierwsza
    return []                                            #wspolrzedna i wynik mnożenia
    

def sumReduce(value1, value2):
    return value1 + value2

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: zadanie2.py <matrix_directory> <vector_directory> <size>", file=sys.stderr)
        exit(-1)

    spark = SparkSession \
        .builder \
        .appName("Zadanie2 - Matrix and Vector multiplication") \
        .getOrCreate()
                            #odpowiednio przypisanie ze standardowego wejscia
    matrix_directory = sys.argv[1]  #sciezki do macierzy
    vector_directory = sys.argv[2]  #sciezki do wektorow
    size = int(sys.argv[3])         #rozmiaru
    temp = -1                       #oraz pomocniczej sluzacej dalej 
                                    #do identyfikacji plikow wektorow

    result = spark.sparkContext.textFile(matrix_directory) \
        .map(lambda line : line.split(';')) \
        .flatMap(lambda row: multiplyMap(row, temp)) \
        .reduceByKey(sumReduce)\
        .sortByKey()
    #1. Czytanie macierzy rzadkich ze std. wejscia
    #2. Podzielenie ich ze wzgledu na ";"
    #3. Mnozenie ich z wektorem
    #4. Redukcja sumujaca
    #5. Sortowanie dla ulatwienia czytania


    for val in result.collect():
        print(val)