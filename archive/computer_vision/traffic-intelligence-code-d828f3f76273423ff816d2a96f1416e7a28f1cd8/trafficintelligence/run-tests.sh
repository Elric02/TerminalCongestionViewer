#!/bin/sh
# for file in tests/*... basename
for f in ./*.py
do
    python3 $f
done
