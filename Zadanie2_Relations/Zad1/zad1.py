from pyspark.sql import SparkSession
import sys

def Map(t):
    t = t.split() #metoda split dzielimy dane wejsciowe
    result = []
    if len(t) != 0: #jesli nie sa puste 
        result.append((t[1],t[1])) #to dopisujemy do listy pare (t,t)
    return result #nastepnie zwracamy przygotowana liste

def Reduce(t):
    key = t[0] #nastepnie pierwszy element przypisujemy do klucza
    return key #i go zwracamy

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: zad1 <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Union")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda a: a[0])
    result = lines.flatMap(Map)\
                        .groupByKey()\
                        .map(Reduce)

    for i in result.collect():
        print("\t"+str(i))

    spark.stop()