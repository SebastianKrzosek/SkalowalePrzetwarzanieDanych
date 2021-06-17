# cat plik1.txt | python3 mapper1.py | python3 reducer1.py
import sys

dictionary = {'\n': " ", '\t': " "} 
# Stworzenie slownika, który potrzebny bedzie nam    
# do zamiany bialych znakow (\n i \t) na spacje

def Mapper(books):
    translation = books.maketrans(dictionary) 
    # Przygotowanie tablicy translacji na podstawie slownika
    words = books.translate(translation).split()
    #translate()- zastepuje znaki w podanym ciagu zgodnie z tablica translacji
    #split() - podzielenie stringa ze wzgledu na spacje
    result = ""
    for i, value in enumerate(words): #petla w ktorej przygotowujemy dane do zwrocenia
        result += value + "\t" + str(1) + "\n" # w postaci: klucz\twartosc\n
    return result

if __name__ == "__main__":
    input = sys.stdin.read() #Czytanie pliku ze standardowego wejscia
    print(Mapper(input))     #Przekazanie tego wejscia jako parametr funkcji