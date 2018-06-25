#!/usr/bin/env scala
import scala.io._

// receive a messy word string and remove punctuation, whitespace and make all lowercase
def normaliseString(s: String) = {
    val normalisedStr: String = s.replaceAll("[^A-Za-z0-9]", "").toLowerCase()
    println(s"$normalisedStr\t1")
    //s.replaceAll("[^A-Za-z0-9]", "").toLowerCase()
    //println(s"$s\t1")
}

// the mapper - receive each line from stdin and split into normalised strings
//for (line <- io.Source.stdin.getLines) {
for (line <- Source.stdin.getLines) {
  line.split(" ").foreach(x => normaliseString(x))
}
