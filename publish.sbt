ThisBuild / organization := "edu.chop.research.dbhi"
ThisBuild / organizationName := "chop"
ThisBuild / organizationHomepage := Some(url("http://www.chop.edu/"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/chop-dbhi/dataexpress"),
    "scm:git@github.com:chop-dbhi/dataexpress.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "masinoa",
    name  = "Aaron Masino"
  ),
  Developer(
    id    = "dcam2015",
    name  = "Diego Campos",
  )
)

ThisBuild / description := "DataExpress is a simple, Scala-based cross database ETL toolkit supporting Postgres, MySql, Oracle, SQLServer, and Sqlite."
ThisBuild / licenses := Seq("BSD-style" -> new URL("http://www.opensource.org/licenses/bsd-license.php"))
ThisBuild / homepage := Some(url("http://dataexpress.research.chop.edu/"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true