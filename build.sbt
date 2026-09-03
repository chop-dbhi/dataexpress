
//assembly to package with dependencies ------------------------------

//standard options ------------------------------

name := "dataexpress"

homepage := Some(url("http://dataexpress.research.chop.edu/"))

val v = "1.0.1"
ThisBuild / version := v
ThisBuild / organization := "edu.chop.research.dbhi"
ThisBuild / scalaVersion := "2.12.20"

licenses := Seq("BSD-style" -> url("http://www.opensource.org/licenses/bsd-license.php"))

assembly / assemblyJarName := s"DataExpress_${v}_standalone.jar"

assembly / test := {}

//compile dependencies------------------------------

libraryDependencies ++= Seq(
  "org.xerial" % "sqlite-jdbc" % "3.50.3.0",
  "org.postgresql" % "postgresql" % "42.7.7",
  "com.mysql" % "mysql-connector-j" % "9.4.0"
)


//test dependencies------------------------------

//Need this for now until we unwind some of the tests
Test / parallelExecution := false


//Only include oracle and sqlserver drivers if testing
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.9" % Test,
  "junit" % "junit" % "4.13.2" % Test,
  "com.oracle.database.jdbc" % "ojdbc11" % "23.9.0.25.07" % Test,
  "com.microsoft.sqlserver" % "mssql-jdbc" % "12.10.0.jre11" % Test
)

Test / parallelExecution := false

//scala options------------------------------

scalacOptions ++= Seq(
  "-language:dynamics",
  "-deprecation"
)

//Publishing options---------------------------

publishMavenStyle := true


// Publishing settings
// new setting for the Central Portal
ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}

Test / publishArtifact := false

pomIncludeRepository := { _ => false }

pomExtra :=
  <scm>
    <url>git@github.com:chop-dbhi/dataexpress.git</url>
    <connection>scm:git:git@github.com:chop-dbhi/dataexpress.git</connection>
  </scm>
  <developers>
    <developer>
      <id>mitalia</id>
      <name>Michael Italia</name>
    </developer>
  </developers>

//console imports------------------------------

Compile / console / initialCommands := """import edu.chop.cbmi.dataExpress.dsl.ETL
import edu.chop.cbmi.dataExpress.dsl.ETL._
import edu.chop.cbmi.dataExpress.dsl.stores.SqlDb
import edu.chop.cbmi.dataExpress.dataModels.RichOption._"""
