#!/usr/bin/env scala
import java.io._

// Generate file of random integer 2-tuples
// Instantiate random number generators, each set by different seed
val bound = new scala.util.Random(10)   // random window bounds
val elem = new scala.util.Random(20)    // random data elements
val winSize = 50                        // max range of random numbers
val numFiles = 10                       // number of files in the stream
val numLines = 100                      // number of lines of data per file
val path = "streamdata/file"

// Generate files of random numbers. Each file will be based
// on a different distribution of numbers according to the bounds
for (fileCounter <- 1 to numFiles) {
    // create and open new data file
    val file = new File(path + fileCounter.toString + ".txt")
    val bw = new BufferedWriter(new FileWriter(file))
    
    // lower bound - random number between 1 and winSize
    val lower1 = bound.nextInt(winSize)
    // upper bound - random number between winSize and 2*winSize
    val upper1 = winSize + bound.nextInt(winSize)
    val lower2 = bound.nextInt(winSize)
    val upper2 = winSize + bound.nextInt(winSize)
    
    // Generate each line of data
    for (lineCounter <- 1 to numLines) {
        // generate random numbers between lower and upper bounds
        var elem1 = lower1 + elem.nextInt(upper1)
        var elem2 = lower2 + elem.nextInt(upper2)
        bw.write("[" + elem1.toString + "," + elem2.toString + "]\n")
    }
    bw.close()
}
