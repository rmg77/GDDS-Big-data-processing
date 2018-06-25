#!/usr/bin/env scala

//Import packages
import scala.io.Source
import java.io._
import java.util.zip._
import scala.collection.mutable.ArrayBuffer

// Define a function that converts GZIP to a stream
def gis(s: String) = new GZIPInputStream(new FileInputStream(s))

// Define an Array of titles (String)    
var sample_titles = ArrayBuffer[String]()

// Define an Array of indices (Int)    
var sample_indices = ArrayBuffer[Int]()

// Add all titles and indices to the arrays
for ((line, index) <- Source.fromInputStream(
    gis(args(0))).getLines().zipWithIndex) {
    sample_titles += line
    sample_indices += index
}

// Print all data
for(i <- sample_titles.indices)
    println(s"sample_titles("+i+") -> ["+sample_indices(i)+"]"+sample_titles(i))
