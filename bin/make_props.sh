#!/bin/sh
exec scala "$0" "$@"
!#
import scala.collection.JavaConversions._
import java.io._

object EnvironmentVariables extends App {

  // FileWriter
  val file = new File("../src/test/resources/test.properties")
  val bw = new BufferedWriter(new FileWriter(file))

  val environmentVars = System.getenv
  for ((k,v) <- environmentVars) bw.write(s"$k=$v\n")

  bw.close()

}

EnvironmentVariables.main(args)
