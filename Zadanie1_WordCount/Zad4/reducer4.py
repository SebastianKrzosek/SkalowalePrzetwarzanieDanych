#!/usr/bin/env python3
import sys
from collections import Counter

def Reducer(books):
    counter = Counter(books) # Stworzenie countera sluzacego do zliczania slow
    result = ""
    for i, value in enumerate(set(books)): # Petla zliczajaca ilosc kluczy
        key = value.split("\t") # Dzielenie po tabulacjach
        #(jako ze wiemy w jakim formacie mapper zwraca dane
        #latwiej jest nam je odpowiednio podzielic)
        result += key[0] + "\t" + str(counter[key[0]+ "\t1"]) + "\n"
    return result

if __name__ == "__main__":
    input = sys.stdin.read().split("\n") #Czytanie pliku ze standardowego wejscia
    print(Reducer(input))    #Przekazanie tego wejscia jako parametr funkcji