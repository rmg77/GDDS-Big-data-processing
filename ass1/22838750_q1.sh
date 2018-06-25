#!/bin/bash

# template for Assignment 1, second file
echo "Ralph Michael Gailis"
echo "22838750"

echo "1. Create a folder on the HDFS and name it after your student number:"
# create a folder on hdfs
hadoop fs -mkdir -p /tmp/22838750

echo "2. List all the files in that folder."
# listing files in hdfs
hadoop fs -ls -R

echo "3. Transfer the above file to the HDFS folder that you just created (use absolute paths)."
# Transferring a file to hdfs using put
hadoop fs -put 22838750_q1.sh /tmp/22838750

echo "4. Print 10 lines (with any preferred order) of the the transferred file which is now stored on HDFS."
# Print 10 lines of transferred file
hadoop fs -cat /tmp/22838750/22838750_q1.sh | head -n 10

echo "
The End!
"
