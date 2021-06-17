from pyspark.sql import SparkSession
import sys

def Map(t):     #MAP z zadania1 
    t = t.split() 
    result = []
    if len(t) != 0:
        result.append((t[1],t[0])) #tylko zwracamy tutaj pare (klucz, wartosc)
    return result

def Reduce(t): 
    key, value = t[0], list(t[1]) 
    result = []
    if 'A' in value and 'B' not in value: 
        result.append(key) 
    return result       

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: zad3 <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Except")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda a: a[0])
    result = lines.flatMap(Map).groupByKey().flatMap(Reduce)

    for i in result.collect():
        print("\t"+str(i))

    spark.stop()
