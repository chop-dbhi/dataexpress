//assembly to package with dependencies ------------------------------

import AssemblyKeys._

assemblySettings

//standard options ------------------------------

name := "dataexpress"

homepage := Some(url("http://dataexpress.research.chop.edu/"))

version := "0.9.1.3"

organization := "edu.chop.research"

scalaVersion := "2.10.0"

licenses := Seq("BSD-style" -> url("http://www.opensource.org/licenses/bsd-license.php"))

//assembly options

jarName in assembly := "DataExpress_" + version.value + "_standalone.jar";

test in assembly := {}

assembleArtifact in packageScala := false


//compile dependencies------------------------------

libraryDependencies ++= Seq(
  "org.xerial" % "sqlite-jdbc" % "3.7.2",
  "postgresql" % "postgresql" % "9.0-801.jdbc4",
  "mysql" % "mysql-connector-java" % "5.1.21"
)


//test dependencies------------------------------

//Need this for now until we unwind some of the tests
parallelExecution in Test := false


libraryDependencies ++= {
  val deps = Seq(
        "org.scalatest" %% "scalatest" % "2.2.6",
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

initialCommands in console := """import edu.chop.cbmi.dataExpress.dsl.ETL
import edu.chop.cbmi.dataExpress.dsl.ETL._
import edu.chop.cbmi.dataExpress.dsl.stores.SqlDb
import edu.chop.cbmi.dataExpress.dataModels.RichOption._"""
