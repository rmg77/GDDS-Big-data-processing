#!/bin/bash
# to run and time this code we do the followings:
# chmod +x 28838750_q3.sh
# time -p ./28838750_q3.sh

# please download the datasets (only once) using:
# git clone https://gist.github.com/3ee4d0a1b7251efd45581d23c9b78c84.git dataset

echo "Ralph Michael Gailis"
echo "28838750"

echo '
// start!
sc.setLogLevel("error") // adjusting the log level to only errors (ignore warnings)

// 1. Create a Spark dataframe from ./dataset/iris.csv then show its content:
val df = spark.read.option("header", "true").csv("./dataset/iris.csv")
df.show(df.count().toInt, false)   // ensures that all rows are printed

// 2- Print the data frame’s schema.
df.printSchema()

// 3- Convert the data frame to an RDD and display its contents.
val myRDD = df.rdd
myRDD.collect.foreach(println)  // prints each row on a separate line

// 4- Create an RDD by reading ./dataset/big.txt and verify its contents by printing the first 5 lines.
val lines = sc.textFile("./dataset/big.txt")
lines.take(5).foreach(println)

// 5- Count the number of chars (including white spaces) in the text file using map and reduce functions
val lineLengths = lines.map(line => line.length)
val numchars = lineLengths.reduce(_+_)

println("End of Q3")
// end!
:quit
'   | spark-shell

