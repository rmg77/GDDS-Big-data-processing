#!/usr/bin/env scala
import scala.io._

// define wordCount Map, to store word strings and their counts in a text stream
var wordCount = scala.collection.immutable.Map[String, Int]()

// perform the wordCount on stdin
//for (ln <- io.Source.stdin.getLines) {
for (ln <- Source.stdin.getLines) {
    var wordOne = ln.split("\t")
    if (wordCount.contains(wordOne(0))){
        wordCount += (wordOne(0) -> (wordCount(wordOne(0)) + wordOne(1).toInt))
    } else {
        wordCount += (wordOne(0) -> wordOne(1).toInt)
    }
}

// print out all the words for reference
wordCount.foreach(println)

// loop over the wordCount Map to find the longest word and the most frequent word
var longestLength = 0            // length of longest word
var longestWord = List[String]() // store List of longest words (could be more than one of equal length)
var mostFreq = 0                 // frequency of most common word
var mostFreqWord = List[Map[String, Int]]()   // store List of most common words and their frequencies

for ((k, v) <- wordCount) {
    if (k.length == longestLength) {
        longestWord ::= k             // add another word to the longest word List
    }
    if (k.length > longestLength) {
        longestWord = List(k)         // we have a new longest word, so restart the List
        longestLength = k.length
    }
    if (v == mostFreq) {
        mostFreqWord ::= Map(k -> v)  // add another word to the most frequent List
    }
    if (v > mostFreq) {
        mostFreqWord = List(Map(k -> v))  // we have a new most frequent word so restart the List
        mostFreq = v
    }
}

// now print out the results
println("Longest word(s): ")
println(longestWord mkString ", ")
println("Most frequent word(s): ")
println(mostFreqWord mkString ", ")

//wordCount.keysIterator.reduceLeft((x,y) => if (x.length > y.length) x else y)
//wordCount.valuesIterator.max