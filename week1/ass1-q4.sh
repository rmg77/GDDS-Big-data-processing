#!/usr/bin/env scala
object main extends App {
    var x = 1
    def y = x
    val z = x
    val t = x + y + z
    x = 2
    var r = t + x + y + z
    println(r)
}

main.main(args)

