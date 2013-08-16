//assembly to package with dependencies ------------------------------

import AssemblyKeys._

assemblySettings

//standard options ------------------------------

name := "dataexpress"

homepage := Some(url("http://dataexpress.research.chop.edu/"))

version := "0.9.1.4-SNAPSHOT"

organization := "edu.chop.research"

scalaVersion := "2.10.0"

licenses := Seq("BSD-style" -> url("http://www.opensource.org/licenses/bsd-license.php"))

//assembly options

jarName in assembly <<=version("DataExpress_" + _ + "_standalone.jar")

test in assembly := {}

assembleArtifact in packageScala := false


//compile dependencies------------------------------

libraryDependencies ++= Seq(
  "org.xerial" % "sqlite-jdbc" % "3.7.2",
  "postgresql" % "postgresql" % "9.0-801.jdbc4",
  "mysql" % "mysql-connector-java" % "5.1.26",
  "com.typesafe" %% "scalalogging-log4j" % "1.0.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.0-beta4"
)


//test dependencies------------------------------

//Need this for now until we unwind some of the tests
parallelExecution in Test := false


libraryDependencies ++= {
  val deps = Seq(
        "org.scalatest" %% "scalatest" % "2.0.M5b",
        "junit" % "junit" % "4.8.1"
      )
  deps map {v => v % "test"}
}

//scala options------------------------------

scalacOptions +="-language:dynamics"


//Publishing options---------------------------

publishMavenStyle := true

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <scm>
    <url>git@github.com:cbmi/dataexpress.git</url>
    <connection>scm:git:git@github.com:cbmi/dataexpress.git</connection>
  </scm>
  <developers>
    <developer>
      <id>mitalia</id>
      <name>Michael Italia</name>
    </developer>
  </developers>)

//console imports------------------------------

javaOptions in console := Seq("-Dlog4j.configuration=log4j2.xml")


initialCommands in console := """import edu.chop.cbmi.dataExpress.dsl.ETL
import edu.chop.cbmi.dataExpress.dsl.ETL._
import edu.chop.cbmi.dataExpress.dsl.stores.SqlDb
import edu.chop.cbmi.dataExpress.dataModels.RichOption._"""
