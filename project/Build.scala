import sbt._
import Keys._

object DataExpressBuild extends Build {
    lazy val dataexpress = Project(id = "DataExpress",
                            base = file("."))

    lazy val dataexpress_oracle = Project(id = "OracleBackend",
                           base = file("dataexpress-oracle")) dependsOn(dataexpress % "test->compile;test->test;compile->compile")

}
