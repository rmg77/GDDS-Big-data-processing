#!/bin/bash
# to run and time this code we do the followings:
# chmod +x 28838750_q4.sh
# time -p ./28838750_q4.sh

# please download the datasets (only once) using:
# git clone https://gist.github.com/3ee4d0a1b7251efd45581d23c9b78c84.git dataset

echo "Ralph Michael Gailis"
echo "28838750"

echo '
// start!
sc.setLogLevel("error") // adjusting the log level to only errors (ignore warnings) 

// Create an RDD by reading ./dataset/big.txt
val lines = sc.textFile("./dataset/big.txt")

/*
6- Find and display the top 100 words of the ./dataset/big.txt file with their frequency ordered from the most frequent to the least.
 The vocabulary should:
- be case-insensitive
- not contain tokens without alphabets eg “711” or “!?” however, “1st” and “5pm” are fine.
- not contain empty words or words contain any space in it eg “” and “   “

Note:
- ignore ‘“?!-_)({}[] symbols in the words eg “students” = “student’s” = “students’” and “#covfefe” = “covfefe” and “stop-word”=stopwords.
- words can be tokenized with any number of whitespace characters.
*/

// import regular expression matching package
import scala.util.matching.Regex

// a function to accept a token and return a purely lower case alphabetic string
def normaliseString(s: String) = {
    // define a regular expression to allow 1st, 2nd,..., 4th,..., 1am, 2am,..., 1pm, 2pm...
    val pattern = new Regex("(\\d*1st)|(\\d*2nd)|(\\d*3rd)|(\\d+th)|(\\d+am)|(\\d+pm)")
    var normalisedStr = ""
    
    // The if condition below checks for an exact once only match of the regular expression. Syntax derived from:
    //   https://stackoverflow.com/questions/3021813/how-to-check-whether-a-string-fully-matches-a-regex-in-scala
    if (pattern.unapplySeq(s).isDefined) {
        normalisedStr = s
    } else {  // remove all non alphabetic characters from the token
        normalisedStr = s.replaceAll("[^A-Za-z]", "").toLowerCase()
    }
    normalisedStr
}

// create a long list of all tokens in the file by splitting over arbitrary whitespace ("\\s+")
val tokenList = lines.flatMap(line => line.split("\\s+"))

// now convert all tokens to lowercase and filter out non-alphabetic tokens
val wordList = tokenList.map(t => normaliseString(t)).filter(word => word.length > 0)
wordList.persist()   // this list will be used several times, so persist in memory

// now count the frequency of each distinct word
val counts = wordList.map(word => (word,1)).reduceByKey(_+_)

// finally sort by value and get the top 100
counts.sortBy(_._2, false).take(100).foreach(println)

/* 
7- Write a program which does word count of the ./dataset/big.txt file using Spark 
 (note that words may be separated by more than one whitespace):
- lowercase all letters
- remove these stop words from the text: [ "a", "about", "above", "after", "again", "against", 
"all", "am", "an", "and", "any", "are", "as", "at", "be", "because", "been", "before", "being", 
"below", "between", "both", "but", "by", "could", "did", "do", "does", "doing", "down", "during", 
"each", "few", "for", "from", "further", "had", "has", "have", "having", "he", "her", "here", "hers", 
"herself", "him", "himself", "his", "how",  "i",  "if", "in", "into", "is", "it", "its", "itself", 
"me", "more", "most", "my", "myself", "nor", "of", "on", "once", "only", "or", "other", "ought", 
"our", "ours", "ourselves", "out", "over", "own", "same", "she", "should", "so", "some", "such", 
"than", "that", "the", "their", "theirs", "them", "themselves", "then", "there", "these", "they", 
"this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "were", "what", 
"when", "where", "where", "which", "while", "who", "who", "whom", "why", "with", "would", "you", 
"your", "yours", "yourself", "yourselves" ]

- ignore quotation marks, digits, #, -,  _ and / in the tokens such that "stop-word"  become "stop-word".
- perform the word count using Spark map and reduce functions
- print the 10 most popular words in descending order along with their counts
- print the 10 least common words in ascending order along with their counts
*/

// A list of words to be filtered out
val stopwords = List("a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "as", "at", "be", "because", "been",
"before", "being", "below", "between", "both", "but", "by", "could", "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from",
"further", "had", "has", "have", "having", "he", "her", "here", "hers", "herself", "him", "himself", "his", "how",  "i",  "if", "in", "into", "is",
"it", "its", "itself", "me", "more", "most", "my", "myself", "nor", "of", "on", "once", "only", "or", "other", "ought",  "our", "ours", "ourselves",
"out", "over", "own", "same", "she", "should", "so", "some", "such", "than", "that", "the", "their", "theirs", "them", "themselves", "then", "there",
"these", "they", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "were", "what", "when", "where", "where",
"which", "while", "who", "who", "whom", "why", "with", "would", "you", "your", "yours", "yourself", "yourselves")

// filter out the stopwords - use the same wordList extracted from Q6
val interestingWords = wordList.filter(word => !stopwords.contains(word))

// perform the map-reduce operation for word counts
val counts = interestingWords.map(word => (word,1)).reduceByKey(_+_)

// print out the 10 most common and 10 least common words and their counts
counts.sortBy(_._2, false).take(10).foreach(println)  // descending order
counts.sortBy(_._2).take(10).foreach(println)         // ascending order

println("The End")
// end!
:quit
'   | spark-shell
