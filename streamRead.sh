#!/bin/bash

# Launch a Spark Shell to implement K-means streaming
echo '
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors

// suppress verbose spark output
sc.setLogLevel("OFF")
sc.stop()

// parameters for the K-means streaming algorithm
val trainingPath = "streamdata"
val numClusters = 4     // number of cluster centres
val batchDuration = 5L  // length between reading each batch
val numDimensions = 2   // two dimensional vector data

// Create a local StreamingContext with 2 working threads 
// The master requires 2 cores to prevent from a starvation scenario.
val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingKMeans")

// Set batch interval of 5 seconds
val ssc = new StreamingContext(conf, Seconds(5))

// K-means streaming code adapted from
//  https://spark.apache.org/docs/latest/mllib-clustering.html#k-means
val trainingData = ssc.textFileStream(trainingPath).map(Vectors.parse)

// Define model with a window memory of half the data (DecayFactor)
val model = new StreamingKMeans().
    setK(numClusters).
    setDecayFactor(0.5).
    setRandomCenters(numDimensions, 50.0)

// train the model and repeatedly output the latest cluster centres
model.trainOn(trainingData)
model.latestModel().clusterCenters.toArray.foreach(println)

// Start the computation
ssc.start()

// Also, wait for 80 seconds or termination signal
// (20 files are being written at a rate of one file every 4 seconds)
ssc.awaitTerminationOrTimeout(80000)
ssc.stop()

// end!
:quit
'   | spark-shell

