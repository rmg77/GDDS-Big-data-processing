#!/bin/bash
# to run and time this code we do the followings:
# chmod +x <student_number>_q4.sh
# time -p ./<student_number>_q4.sh

# please download the datasets (only once) using:
# git clone https://gist.github.com/3ee4d0a1b7251efd45581d23c9b78c84.git dataset

echo "Student Full Name"
echo "Student Number"

echo '
// start!
sc.setLogLevel("error") // adjusting the log level to only errors (ignore warnings) 

// Create an RDD by reading ./dataset/big.txt
// val lines =...

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

//lines...

/* 
7- Write a program which does word count of the ./dataset/big.txt file using Spark 
 (note that words may be separated by more than one whitespace):
- lowercase all letters
- remove these stop words from the text: [ "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "could", "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from", "further", "had", "has", "have", "having", "he", "her", "here", "hers", "herself", "him", "himself", "his", "how",  "i",  "if", "in", "into", "is", "it", "its", "itself", "me", "more", "most", "my", "myself", "nor", "of", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "she", "should", "so", "some", "such", "than", "that", "the", "their", "theirs", "them", "themselves", "then", "there", "these", "they", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "were", "what", "when", "where", "where", "which", "while", "who", "who", "whom", "why", "with", "would", "you", "your", "yours", "yourself", "yourselves" ]
- ignore quotation marks, digits, #, -,  _ and / in the tokens such that "stop-word"  become "stop-word".
- perform the word count using Spark map and reduce functions
- print the 10 most popular words in descending order along with their counts
- print the 10 least common words in ascending order along with their counts
*/

//lines...

println("End End")
// end!
:quit
'   | spark-shell

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
