#!/usr/bin/env scala

// Create an Array of 1 to 100 Int to simulate a stream
var stream = Array.range(1, 101)
val N = stream.length

// Set the length of the reservoir
val n = 10

// Book some space for the receiving stream
val reservoir = new Array[Int](n)

// Create a random number generator
val myrandnum = scala.util.Random
myrandnum.setSeed(5202)

for (k <- 0 until N) {
    // Store the first k numbers
    if (k < n) {
        reservoir(k) = stream(k)
    }
    else {
        // Pick a random index.
        val t = myrandnum.nextInt(k)
        // replace t-th element if t is a valid index
        if (t < n) {
            reservoir(t) = stream(k)
        }
    }
}

println("These " + n.toString + " values: ")
println(reservoir.mkString(" "))
println("are randomly selected from these " + N.toString + "values: ")
println(stream.mkString(" "))
