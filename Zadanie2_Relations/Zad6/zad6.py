from pyspark.sql import SparkSession
import sys

def Map(t, header, columns): #wybieramy kolumny ktore sa na liscie podanej w argv
    return t, tuple(t[header.index(c)] for c in columns)

def Reduce(t):
    value = list(t[1])
    return value

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: zad6 <file> <columns>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Projection")\
        .getOrCreate()

    lines = spark.read.option('header', True).csv(sys.argv[1])
    header = [i.name for i in lines.schema.fields]
    columns = sys.argv[2:]

    result = lines.rdd.map(tuple).\
            map(lambda a: Map(a, header, columns)).\
            groupByKey().map(Reduce)

    print()
    for i in result.collect():
        print(i)
    print()
    spark.stop()

