#!/bin/bash

echo "Ralph Michael Gailis"
echo "28838750"

# prepare a streaming data directory and ensure it is empty
mkdir -p streamdata
rm streamdata/*

# start a Scala shell to read the stream of data and run it
#  in the background
./streamRead.sh &

count=1
# create a stream of 20 files of random vectors
while [ $count -le 20 ]
do
# script fileWrite.scala appends $count to the written file name
scala fileWrite.scala $count
# space each file write by 4 seconds
sleep 4
(( count++ ))
done
