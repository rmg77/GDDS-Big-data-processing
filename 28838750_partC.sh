#!/bin/bash
# to run and time this code we do the followings:
# chmod +x 28838750_partC.sh
# time -p ./28838750_partC.sh

echo "Ralph Michael Gailis"
echo "28838750"

echo '
// Assessment 4 - Part C

import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types.{StructType, StructField, StringType,
    IntegerType, LongType}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline

// adjusting the log level to only errors (ignore warnings)
sc.setLogLevel("error")

val customSchema = StructType(Array(
    StructField("DayOfMonth", IntegerType, true),
    StructField("DayOfWeek", IntegerType, true),
    StructField("CarrierCode", StringType, true),
    StructField("TailNum", StringType, true),
    StructField("FlightNum", StringType, true),
    StructField("OriginAirportID", LongType, true),
    StructField("OriginAirport", StringType, true),
    StructField("DestinationAirportID", LongType, true),
    StructField("DestinationAirport", StringType, true),
    StructField("ScheduledDepartureTime", StringType, true),
    StructField("ActualDepartureTime", StringType, true),
    StructField("DepartureDelay", IntegerType, true),
    StructField("ScheduledArrivalTime", StringType, true),
    StructField("ActualArrivalTime", StringType, true),
    StructField("ArrivalDelay", StringType, true),
    StructField("ElapsedTime", IntegerType, true),
    StructField("Distance", IntegerType, true)
))
    
val df = spark.read.schema(customSchema).csv("test.csv.bz2")
df.printSchema()

/************************/
// Q1 - Sort the flights based on their flight number and scheduled
//  departure time
val sortedDF = df.orderBy("FlightNum", "ScheduledDepartureTime")

// Transform categorical columns to indices for regression
// Code snippet from
//  http://apache-spark-user-list.1001560.n3.nabble.com/StringIndexer-on-several-columns-in-a-DataFrame-with-Scala-td29842.html
val categoricalCol = Array("CarrierCode", "FlightNum", "OriginAirportID",
    "DestinationAirportID", "ScheduledDepartureTime")
val indexers = categoricalCol.map { colName =>
  new StringIndexer().setInputCol(colName).setOutputCol(colName + "_indexed")
}

// Apply indexers to training data
val pipeline = new Pipeline().setStages(indexers)      
val indexedDF = pipeline.fit(sortedDF).transform(sortedDF)
// we will be accessing the dataframe multiple times so persist in memory
indexedDF.persist()

// Assemble the feature vector based on selected attributes
val assembler = new VectorAssembler().
    setInputCols(Array("DayOfMonth", "DayOfWeek", "CarrierCode_indexed",
    "FlightNum_indexed", "OriginAirportID_indexed",
    "DestinationAirportID_indexed", "ScheduledDepartureTime_indexed")).
    setOutputCol("features")

// Perform transform to insert feature vector and add a label column
val finalDF = assembler.transform(indexedDF).
  withColumn("label", indexedDF("ArrivalDelay") - indexedDF("DepartureDelay")).
  select("label", "features")
finalDF.persist()
finalDF.take(10).foreach(println)

/**************************/
// Q2 - Split into train-test dataframes
// We now have a regression-ready dataset.
// Filtering code for splitting dataframe adapted from:
//  https://stackoverflow.com/questions/46991818/reduce-size-of-spark-dataframe-by-selecting-only-every-n-th-element-with-scala
val n = 10
val testDF = finalDF.withColumn("index", row_number().  // add an index column
    over(Window.orderBy(monotonically_increasing_id))).
    filter($"index" % n === 0).       // filter modulo n to select only those rows
    drop("index")                     // now drop index column again
    
val trainDF = finalDF.except(testDF) // training data is the remainder
println("Total DF size = " + finalDF.count)
println("Train DF size = " + trainDF.count)
println("Test DF size = " + testDF.count)

/**************************/
// Q3 - Train a linear regression to model the training dataset and predict
// the overall delay. Code adapted from
//  https://spark.apache.org/docs/latest/ml-classification-regression.html#linear-regression
val lr = new LinearRegression().
    setMaxIter(10).
    // these parameters need to be zero to undertake "normal" solver
    setRegParam(0). 
    setElasticNetParam(0).  // normal solver is required for p-values
    setFeaturesCol("features").
    setLabelCol("label").
    setPredictionCol("prediction")
// For a discussion of solvers and requirements for extracting p-values, see
//  https://stackoverflow.com/questions/46696378/spark-linearregressionsummary-normal-summary

// Fit the model
val lrModel = lr.fit(trainDF)

// Print the coefficients and intercept for linear regression
println(s"Coefficients: ${lrModel.coefficients}")
println(s"Intercept: ${lrModel.intercept}")

/**************************/
// Q4 - Calculate and report train and test error (RMSE)
val lrPrediction = lrModel.transform(testDF)

// Calculate residuals of model prediction vs data
val residuals = lrPrediction.select("label", "prediction").
    // calculate difference of data label and prediction
    withColumn("diff", lrPrediction("label") - lrPrediction("prediction")).
    // extract value from row
    select("diff").rdd.map(r => r(0).toString.toDouble).collect()
val n = residuals.length

// Calculate the root mean square error of the model
val testRMSE = math.sqrt(
    residuals.map( x => math.pow(x, 2) ).reduceLeft(_+_) / n
)

// Print model error results
val trainingSummary = lrModel.summary
println(s"Training RMSE = ${trainingSummary.rootMeanSquaredError}")
println(s"Training r2 = ${trainingSummary.r2}")
println("Test RMSE = " + testRMSE)

/**************************/
// Q5 - Identify the most and least contributing attributes in the training set.
// This is done by examining p-values and t-statistics
println("p-values: ")
trainingSummary.pValues
println("t-statistics: ")
trainingSummary.tValues
// See a discussion of these values in the assessment report

// end!
:quit
'   | spark-shell
