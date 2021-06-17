#!/bin/sh
cat ../plik1.txt | python3 mapper3.py | sort | python3 reducer3.py