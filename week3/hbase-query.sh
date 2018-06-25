#!/bin/bash

function getVals {
    echo "scan 'sales', {COLUMNS => 'transaction:Price', LIMIT => 10}
    " | hbase shell | awk -F, '{print $NF}' | awk -F= '{if ($1==" value") print $2}'
}
vals=($(getVals))
echo "${vals[@]}"

sum=0
for i in "${vals[@]}"
do
   (( sum += $i ))
   #sum=`expr $sum + 1`
   #sum=$((sum+$i))
done
echo "sum = $sum"
