//assembly to package with dependencies ------------------------------

//standard options ------------------------------

name := "dataexpress"

homepage := Some(url("http://dataexpress.research.chop.edu/"))

val v = "0.9.3"

version := v

scalaVersion := "2.12.8"

useGpg := true

licenses := Seq("BSD-style" -> url("http://www.opensource.org/licenses/bsd-license.php"))

assemblyJarName in assembly := s"DataExpress_${v}_standalone.jar"

test in assembly := {}

assembleArtifact in assemblyPackageScala := false

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
        "org.scalatest" %% "scalatest" % "3.0.8",
        "junit" % "junit" % "4.8.1"
      )
  deps map {v => v % "test"}
}

//scala options------------------------------

scalacOptions +="-language:dynamics"

//console imports------------------------------

initialCommands in console := """import edu.chop.cbmi.dataExpress.dsl.ETL
import edu.chop.cbmi.dataExpress.dsl.ETL._
import edu.chop.cbmi.dataExpress.dsl.stores.SqlDb
import edu.chop.cbmi.dataExpress.dataModels.RichOption._"""
