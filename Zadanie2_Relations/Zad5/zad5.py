from pyspark.sql import SparkSession
import sys

def Map(t, header, column, operation, value): 
    index = header.index(column)
    result = []
    if operation == '=' and float(t[index]) == float(value):
        result.append(t)
    elif operation == '!=' and float(t[index]) != float(value):
        result.append(t)
    elif operation == '>' and float(t[index]) > float(value):
        result.append(t)
    elif operation == '>=' and float(t[index]) >= float(value):
        result.append(t)
    elif operation == '<' and float(t[index]) < float(value):
        result.append(t)
    elif operation == '<=' and float(t[index]) <= float(value):
        result.append(t)
    elif operation.lower() == 'like' and value.lower() in t[index].lower():
        result.append(t)
    return result


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: zad5 <file> <column> <operation> <value>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Select")\
        .getOrCreate()

    lines = spark.read.option('header', True).csv(sys.argv[1])
    
    header = [i.name for i in lines.schema.fields]
    column = sys.argv[2]
    operation = sys.argv[3]
    value = sys.argv[4]

    result = lines.rdd.map(tuple).\
        flatMap(lambda a: Map(a, header, column, operation, value))

    print()
    for i in result.collect():
        print(i)
    print()
    spark.stop()
