#!/bin/bash
# to run and time this code we do the followings:
# chmod +x <student_number>_q.sh
# time -p ./<student_number>_q.sh

echo '
val lines = sc.textFile("/srv/home/rgai0001/week4/simpleGraph.txt")
val N = lines.count  // number of URLs

val links = lines.map(line => line.split("\\s+")).map(line => (line.head, line.tail)) // RDD of (URL, outlinks)
links.collect()
var ranks = links.map(link => (link._1, 1.0/N))  // RDD of (URL, rank) pairs, all initialised equally
ranks.collect()

val maxIter = 5 // Maximum number of updates
val alpha = 0.15  // update coefficient
for (i <- 1 to maxIter) {
    // Build an RDD of (outlink, newContribution) pairs
    val contribs = links.join(ranks).values.flatMap{
        case (urls, rank) => val size = urls.size
        urls.map(url => (url, rank / size))
    }
    contribs.collect().foreach(println)
    println()

    // Sum contributions by URL and get new ranks
    ranks = contribs.reduceByKey(_+_).mapValues(sum => alpha/N + (1-alpha)*sum)
}

ranks.collect().foreach(println)
' | spark-shell
