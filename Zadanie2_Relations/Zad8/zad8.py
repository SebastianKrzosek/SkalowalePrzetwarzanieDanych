from pyspark.sql import SparkSession
import sys

def Map(t): 
    t = t.split("\t") 
    result = []
    if t[0] == 'Customers': #sprawdzamy z jakimi danymi mamy do czynienia
        result.append((t[1], (t[0], t[1:]))) #zwracamy odpowiednie kolumny z Customers
    elif t[0] == 'Orders':
        result.append((t[2],(t[0], t[1], t[3]))) #ewentualnie z Orders
    return result

def Reduce(t, direct):  #REDUCE z zadania 4
    value = list(t[1]) 
    cust = []
    orders = []
    result = []

    for v in value:
        if v[0] == 'Customers':
            cust.append(v)
        elif v[0] == 'Orders':
            orders.append(v)

    if direct.lower() == 'left' and len(cust) == 0: #z warunkiem left 
        cust.append(('Customers',[]))
    elif direct.lower() == 'right' and len(orders) == 0: #i right joina
        orders.append(('Orders',[]))

    for i, customer in enumerate(cust):
        for j, order in enumerate(orders):
            result.append([customer, order])
    return result      
    
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: zad8 <file> <direction>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Union")\
        .getOrCreate()

    direct = sys.argv[2]

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda a: a[0])
    result = lines.flatMap(Map).groupByKey().flatMap(lambda a: Reduce(a, direct))
    print()
    for i in result.collect():
        print(i)
    print()
    spark.stop()
 
