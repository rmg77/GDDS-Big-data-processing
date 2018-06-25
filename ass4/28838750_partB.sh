#!/bin/bash
# to run and time this code we do the followings:
# chmod +x 28838750_partB.sh
# time -p ./28838750_partB.sh

echo "Ralph Michael Gailis"
echo "28838750"

echo '
// start!
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._

// file name stub to use to write files to
val path = "28838750_"

// adjusting the log level to only errors (ignore warnings)
sc.setLogLevel("error") 

// schema definition modelled on code at https://github.com/databricks/spark-csv
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
// we will be accessing the dataframe multiple times so persist in memory
df.persist()
df.printSchema()

// select out the airport IDs (to be used as vertex IDs) and airport codes
val airportOrigins = df.select($"OriginAirportID", $"OriginAirport")
val airportDests = df.select($"DestinationAirportID", $"DestinationAirport")

// contruct an RDD of airport vertices by merging the origin and destination airport data into a single
//   dataframe, converting to RDD, and finding unique values
val airports = airportOrigins.union(airportDests).toDF("AirportID", "AirportCode").distinct()
println("Unique airports: " + airports.count())
airports.show(10)

// Define default vertex
val defaultAirport = ("Missing")

// Construct the graph vertices from the airport ID numbers and airport codes.
// When converting from dataframe to RDD, each type is cast to Any, so
// ".toString.toLong" is required to cast the vertex ID to a Long.
val airportVertices: RDD[(VertexId, String)] = airports.rdd.map(x => (x(0).toString.toLong, x(1).toString))
airportVertices.take(10)

// Extract vertex IDs for airport origins and destinations and verify that
//   each pair has many flights between them
val flightsFromTo = df.select($"OriginAirportID", $"DestinationAirportID", $"ElapsedTime", $"DepartureDelay", $"ArrivalDelay")
//println("Total number of flights:    " + flightsFromTo.count())
//println("Number of distinct flights: " + flightsFromTo.distinct.count())

val flightEdges = flightsFromTo.rdd.map(x => Edge(
    x(0).toString.toLong,    // origin airport ID
    x(1).toString.toLong,    // arrival aiport ID
    // Define Edge property - (ElapsedTime, DepartureDelay, ArrivalDelay)
    (x(2).toString.toInt, x(3).toString.toInt, x(4).toString.toInt)
))
flightEdges.take(10)

// Now create the airport-flights graph
val flightGraph = Graph(airportVertices, flightEdges, defaultAirport)
// we will be performing multiple queries, so persist in memory
flightGraph.persist()

/*-------------*/
// Q1 - Find the top 10 busiest airports.
// Define the busiest airport to be the one with the most in-degrees plus
//   out-degrees, i.e. total number of flights in and out.
val airportDegree = flightGraph.inDegrees.join(flightGraph.outDegrees).
    map(x => (x._1, x._2._1 + x._2._2)).    // sum inDeg + outDeg for each vertex
    join(airportVertices).                  // join with vertices RDD so vertex names can be attached
    sortBy(_._2._1, ascending=false)        // sort descending order

// display results and print to file
airportDegree.take(10).map(_._2).foreach(println(_))
val pw = new PrintWriter(new File(path + "partB_1.csv"))
pw.write("NumberFlights,AirportCode\n")
airportDegree.take(10).map(_._2).foreach(pw.println)
pw.close

/*-------------*/
// Q2 - The top 10 routes based on actual time in air

// Actual time in air =  Elapsed Time - DepartuareDelay + ArrivalDelay
//   actual time for each edge calculated from edge attributes
val actualTime = flightGraph.mapEdges(e => e.attr._1 - e.attr._2 + e.attr._3).
    groupEdges(_+_).                                    // aggregate time for all directed edges between two vertices
    triplets.map(t => (t.srcAttr, t.dstAttr, t.attr)).  // extract vertex and total actual time information
    sortBy(_._3, ascending=false)                       // sort in descending order

// display results and print to file
actualTime.take(10).foreach(println(_))
val pw = new PrintWriter(new File(path + "partB_2.csv"))
pw.write("OriginAirport,DestinationAirport,ActualTimeInAir\n")
actualTime.take(10).foreach(pw.println)
pw.close

/*-------------*/
// Q3 - The 10 worst routes in terms of average arrival delays

// get arrival delay from edge attribute and insert edge counter
val arrivalDelay = flightGraph.mapEdges(e => (e.attr._3, 1)).
    groupEdges((e1, e2) => (e1._1 + e2._1, e1._2 + e2._2)). // aggregate arrival delays for each route and count like edges
    mapEdges(e => e.attr._1.toFloat / e.attr._2).           // average = total arrival delay / number of edges per route
    triplets.map(t => (t.srcAttr, t.dstAttr, t.attr)).      // extract vertex and average arrival delay information
    sortBy(_._3, ascending=false)                           // sort in descending order

// display results and print to file    
arrivalDelay.take(10).foreach(println(_))
val pw = new PrintWriter(new File(path + "partB_3.csv"))
pw.write("OriginAirport,DestinationAirport,AverageArrivalDelay\n")
arrivalDelay.take(10).foreach(pw.println)
pw.close

/*-------------*/
// This code is based on GraphX documentation code and StackExchange code
//   snippet:
//    https://spark.apache.org/docs/latest/graphx-programming-guide.html#neighborhood-aggregation
//    https://stackoverflow.com/questions/35648558/how-to-sum-edge-weights-with-graphx
// with some modifications for the specific problem
val departureDelays: RDD[(VertexId, (Int, Int))] = 
    flightGraph.mapEdges(e => e.attr._2).          // collapse edge attribute to departure delay
    aggregateMessages[(Int, Int)](
        triplet => { // Map Function
            triplet.sendToSrc((1, triplet.attr))  // send edge counter and departure delay to source
        }, // Reduce Function
        (a, b) => (a._1 + b._1, a._2 + b._2),     // count edges and aggregate departure delays
        TripletFields.EdgeOnly
    )

// Divide total departure delays by number of edges (flights)
val avgDepartureDelays: RDD[(VertexId, Double)] =
    departureDelays.mapValues( {
        case (count, totalDelay) => totalDelay.toFloat / count
    } )//.join(airportVertices)            // join with vertices RDD so vertex names can be attached
//      .sortBy(_._2._1, ascending=false)  // sort descending order
        
// display results and print to file
avgDepartureDelays.take(10).foreach(println(_))
val pw = new PrintWriter(new File(path + "partB_4.csv"))
pw.write("OriginAirport,AverageDepartureDelay\n")
avgDepartureDelays.take(10).foreach(pw.println)
pw.close

// end!
:quit
'   | spark-shell
