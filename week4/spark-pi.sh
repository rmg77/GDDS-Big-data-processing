#!/bin/bash
# to run and time this code we do the followings:
# chmod +x <student_number>_q.sh
# time -p ./<student_number>_q.sh

# please download the datasets (only once) using:
# git clone https://gist.github.com/3ee4d0a1b7251efd45581d23c9b78c84.git dataset

echo "Student Full Name"
echo "Student Number"

echo '
// start!
sc.setLogLevel("error") // adjusting the log level to only errors (ignore warnings) 
// 1- Create an RDD by reading /etc/passwd and verify its contents by printing the first 5 lines.
// val lines =...
val lines = sc.textFile("/etc/passwd")
lines.take(5).foreach(println)
// 2- Write a short script to approximate Pi (taken from https://spark.apache.org/examples.html):
val NUM_SAMPLES = 10000
var count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
  val x = math.random
  val y = math.random
  x*x + y*y < 1
}.count()
println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")
println("End of Spark example")
// end!
:quit
' | spark-shell
