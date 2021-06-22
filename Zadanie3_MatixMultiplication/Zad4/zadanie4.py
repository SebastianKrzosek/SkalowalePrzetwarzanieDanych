from __future__ import print_function
from pyspark.sql import SparkSession
import sys

def map_1(cell, column):
    x = cell[0]     #przypisujemy wspolrzedne 
    y = cell[1]     #x i y komorki
    value = cell[2] #oraz jej wartosc
    result = []     
    for index in range(column): #i generujemy pary klucz wartosc
        result.append(((int(x), index + 1), ('A', int(y), float(value))))
    return result               #dla macierzy pierwszej w postaci
                                # ((i,column),('A',j, a_ij))

def map_2(cell, rows):
    x = cell[0]     #przypisujemy wspolrzedne 
    y = cell[1]     #x i y komorki
    value = cell[2] #oraz jej wartosc
    result = []
    for index in range(rows):   #i generujemy pary klucz wartosc
        result.append(((index + 1, int(y)), ('B', int(x), float(value))))
    return result               #dla macierzy drugiej w postaci
                                # ((row, k),('A',j, b_jk))

def Reducer(cells):
    x = cells[0][0]   #przypisujemy wspolrzedne 
    y = cells[0][1]   #x i y
    values = cells[1] #oraz wartosci
    value_dict = dict()
    result = float(0)
    
    for val in values: #przechodzac po wartosciach
        if val[1] not in value_dict:    #sprawdzamy czy znajdują się w slowniku
            value_dict[val[1]] = val[2] #jesli nie, to dopisujemy je w odpowiednie miejsce
        else:
            result += value_dict[val[1]] * val[2]
                        #natomiast jesli sie znajduja, to dokonujemy od razu sumy na iloczynie
    if result != 0: #jesli wynik tego jest rozny od zera, to zwracamy wspolrzene wraz z wartoscia
        return [((x,y), result)]
    else:
        return []   #w przeciwnym razie zwracamy pusty element


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: <A_directory> <B_directory> <A_rows_num> <B_columns_num>", file=sys.stderr)
        exit(-1)

    spark = SparkSession \
        .builder \
        .appName("MatrixProduct") \
        .getOrCreate()

    rows_A = int(sys.argv[3])   #Podajemy liczbe wierzy
    cols_B = int(sys.argv[4])   #oraz liczbe kolumn

    A = spark.sparkContext.textFile(sys.argv[1]) \
        .map(lambda line : line.split(';')) \
        .flatMap(lambda cell: map_1(cell, cols_B))
    #1. Czytamy macierz rzadka A ze std. wejscia
    #2. Dzielimy dane ze wzgledu na znak ";"
    #3. Przygotowujemy odpowiednio pary (klucz,wartosc) dla macierzy A

    B = spark.sparkContext.textFile(sys.argv[2]) \
        .map(lambda line : line.split(';')) \
        .flatMap(lambda cell: map_2(cell, rows_A))
    #1. Czytamy macierz rzadka B ze std. wejscia
    #2. Dzielimy dane ze wzgledu na znak ";"
    #3. Przygotowujemy odpowiednio pary (klucz,wartosc) dla macierzy B


    result = A.union(B) \
        .groupByKey() \
        .flatMap(Reducer)\
        .sortByKey()
    #1. Dokonujemy unii na macierzach A i B
    #2. Grupujemy po kluczu
    #3. Dokonujemy wykonujemy cale mnożenie macierzy
    #4. Sortujemy dla ulatwienia czytania

    for val in result.collect():
        print(val)
