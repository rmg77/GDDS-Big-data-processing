#!/bin/bash
# to run and time this code we do the followings:
# chmod +x <student_number>_q3.sh
# time -p ./<student_number>_q3.sh

# please download the datasets (only once) using:
# git clone https://gist.github.com/3ee4d0a1b7251efd45581d23c9b78c84.git dataset

echo "Student Full Name"
echo "Student Number"

echo '
// start!
sc.setLogLevel("error") // adjusting the log level to only errors (ignore warnings) 
// 1. Create a Spark dataframe from ./dataset/iris.csv then show its content:
// val df = ...
// df...

// 2- Print the data frame’s schema.
// df...

// 3- Convert the data frame to an RDD and display its contents.
// val myRDD =...
// myRDD...


// 4- Create an RDD by reading ./dataset/big.txt and verify its contents by printing the first 5 lines.
// val lines =...


// 5- Count the number of chars (including white spaces) in the text file using map and reduce functions
// val lineLengths = ...

println("End of Q4")
// end!
:quit
'   | spark-shell
