# bin/spark-submit ./Zad4/wordcount.py ./Zad4/plik1.txt 
from __future__ import print_function
import sys
from operator import add
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file.txt>", file=sys.stderr)
        exit(-1)
    
    spark = SparkSession\
        .builder\
            .appName("PythonWordCount")\
                .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    countsR = lines.pipe("/home/krzosek/Dokumenty/Spark/spark-3.0.2-bin-hadoop2.7/Zad4/mapper4.py")\
        .sortBy(lambda line: line.split("\t")[0])\
           .pipe("/home/krzosek/Dokumenty/Spark/spark-3.0.2-bin-hadoop2.7/Zad4/reducer4.py")
    
    for line in countsR.collect():
        print(line)
    
    spark.stop()