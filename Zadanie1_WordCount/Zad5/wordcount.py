from __future__ import print_function
from operator import add
from pyspark.sql import SparkSession
from nltk.stem import WordNetLemmatizer 
import sys

wnl = WordNetLemmatizer() 
stopwords = set(["a", "about", "above", "after", "again", "against", "ain", "all", "am", "an", "and", "any", "are", "aren", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can", "couldn", "couldn't", "d", "did", "didn", "didn't", "do", "does", "doesn", "doesn't", "doing", "don", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn", "hadn't", "has", "hasn", "hasn't", "have", "haven", "haven't", "having", "he", "her", "here", "hers", "herself", "him", "himself", "his", "how", "i", "if", "in", "into", "is", "isn", "isn't", "it", "it's", "its", "itself", "just", "ll", "m", "ma", "me", "mightn", "mightn't", "more", "most", "mustn", "mustn't", "my", "myself", "needn", "needn't", "no", "nor", "not", "now", "o", "of", "off", "on", "once", "only", "or", "other", "our", "ours", "ourselves", "out", "over", "own", "re", "s", "same", "shan", "shan't", "she", "she's", "should", "should've", "shouldn", "shouldn't", "so", "some", "such", "t", "than", "that", "that'll", "the", "their", "theirs", "them", "themselves", "then", "there", "these", "they", "this", "those", "through", "to", "too", "under", "until", "up", "ve", "very", "was", "wasn", "wasn't", "we", "were", "weren", "weren't", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", "won", "won't", "wouldn", "wouldn't", "y", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "", "yourselves", "could", "he'd", "he'll", "he's", "here's", "how's", "i'd", "i'll", "i'm", "i've", "let's", "ought", "she'd", "she'll", "that's", "there's", "they'd", "they'll", "they're", "they've", "we'd", "we'll", "we're", "we've", "what's", "when's", "where's", "who's", "why's", "would"])
dictionary = {"\t":" ","\n": " ",".": " ",",": " ",";": " ", ":": " ","‘":" ","’":" ", "-": "", "?": " ", "!": " ", "(": " ", ")": " ", "[": " ", "]": " ", ",": " ", "\r":"", "_":"", "⎪":"", "⎫":"", "⎬":" ", "⎭":" ", "—":" ","=":" ",u'\ufeff':"", '\"': "", '"':"", '“':"", '”':""}

def Map(r):
    translation = r[0].maketrans(dictionary)
    words = r[0].translate(translation).lower().split()
    result = [(wnl.lemmatize(word), 1) for word in words if word not in stopwords]
    return result

def Reduce(r):
    key, value = r[0], r[1]
    return (key, sum(list(value)))
    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bin/spark-submit word_count.py <file.txt>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd
    countsR = lines.flatMap(Map)\
                        .groupByKey()\
                        .map(Reduce)\
                        .sortByKey()

    for result in countsR.collect():
        print(result)
    spark.stop()