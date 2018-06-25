#!/bin/bash
# to run and time this code we do the followings:
# chmod +x <student_number>_q.sh
# time -p ./<student_number>_q.sh

# pass a Spark script to the shell
echo '
// start!

// Import the packages
import org.apache.spark._
import org.apache.spark.streaming._

// Force Spark to talk less and work more!
sc.setLogLevel("OFF")
sc.stop()

// Create a local StreamingContext with 2 working threads 
// The master requires 2 cores to prevent from a starvation scenario.
val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

// Set batch interval of 1 second.
val ssc = new StreamingContext(conf, Seconds(1))

// Create a DStream that will connect to hostname:port, like localhost:9999
val lines = ssc.socketTextStream("localhost", 9999)

// Split each line into words
val words = lines.flatMap(_.split(" "))

// Count each word in each batch
//val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
// Count words in 30 sec windows
val wordCounts = words.map(word => (word, 1)).reduceByKeyAndWindow(
    _+_, Seconds(10), Seconds(2))

// Print the results
wordCounts.print()

// Start the computation
ssc.start()

// Also, wait for 10 seconds or termination signal
ssc.awaitTerminationOrTimeout(1000)
ssc.stop()

println("End of Spark example")
// end!
:quit
' | spark-shell
