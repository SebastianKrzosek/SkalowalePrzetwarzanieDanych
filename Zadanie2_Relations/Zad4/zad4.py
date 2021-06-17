from pyspark.sql import SparkSession
import sys

def Map(t):
    t = t.split("\t") #podzial danych
    result = []
    if t[0] == 'Customers': #sprawdzamy z jakimi danymi mamy do czynienia
        result.append((t[1], (t[0], t[1:]))) #zwracamy odpowiednie kolumny z Customers
    elif t[0] == 'Orders':
        result.append((t[2],(t[0], t[1], t[3]))) #ewentualnie z Orders
    return result

def Reduce(t):
    value = list(t[1]) 
    cust = []
    orders = []
    result = []
    for v in value:
        if v[0] == 'Customers':
            cust.append(v)
        elif v[0] == 'Orders':
            orders.append(v)

    for i, customer in enumerate(cust):
        for j, order in enumerate(orders):
            result.append([customer, order])
    return result       

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: zad4 <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("InnerJoin")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda a: a[0])
    result = lines.flatMap(Map).groupByKey().flatMap(Reduce)
    print("")
    for i in result.collect():
        print(i)
    print()
    spark.stop()
