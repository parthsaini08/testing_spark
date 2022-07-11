#!/bin/bash
filecontent=( `cat "operationList.txt" `)
cd ./src

for t in "${filecontent[@]}"
do
python3 spark_processor.py -o $t &
echo "Started "$t
done
echo "Started all operations!"
