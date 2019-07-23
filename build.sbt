//assembly to package with dependencies ------------------------------

//standard options ------------------------------

name := "dataexpress"

homepage := Some(url("http://dataexpress.research.chop.edu/"))

val v = "0.9.3"

version := v

organization := "edu.chop.research.dbhi"

scalaVersion := "2.12.8"

licenses := Seq("BSD-style" -> url("http://www.opensource.org/licenses/bsd-license.php"))

assemblyJarName in assembly := s"DataExpress_${v}_standalone.jar"

test in assembly := {}

assembleArtifact in packageScala := false

//compile dependencies------------------------------

libraryDependencies ++= Seq(
  "org.xerial" % "sqlite-jdbc" % "3.28.0",
  "mysql" % "mysql-connector-java" % "8.0.16"
)


//test dependencies------------------------------

//Need this for now until we unwind some of the tests
parallelExecution in Test := false


libraryDependencies ++= {
  val deps = Seq(
        "org.scalatest" %% "scalatest" % "2.2.4",
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
    <url>git@github.com:chop-dbhi/dataexpress.git</url>
    <connection>scm:git:git@github.com:chop-dbhi/dataexpress.git</connection>
  </scm>
  <developers>
    <developer>
    <id>masinoa</id>
    <name>Aaron Masino</name>
  </developer>
  <developer>
    <id>dcam2015</id>
    <name>Diego Campos</name>    
  </developer>
  </developers>)

//console imports------------------------------

initialCommands in console := """import edu.chop.cbmi.dataExpress.dsl.ETL
import edu.chop.cbmi.dataExpress.dsl.ETL._
import edu.chop.cbmi.dataExpress.dsl.stores.SqlDb
import edu.chop.cbmi.dataExpress.dataModels.RichOption._"""
