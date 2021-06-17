from pyspark.sql import SparkSession
import sys
import statistics

def Map(t, header, columns): #wybieramy kolumny ktore sa na liscie podanej w argv
    return tuple(t[header.index(c)] for c in columns)

def Reduce(t):
    key = t[0]
    value = [float(i[1:]) for i in t[1]]
    return "\n"+str(key) + ":"+"\nSum: " + str(sum(value))+ "\nMean: " + \
        str(statistics.mean(value))+"\nMinimum: " + str(min(value))+ \
        "\nMaximum: " + str(max(value))+"\nLenght: " + str(len(value))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: zad6 <file> <columns>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Grouping&Agregation")\
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
