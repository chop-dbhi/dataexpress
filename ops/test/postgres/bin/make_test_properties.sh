#!/bin/sh
exec scala "$0" "$@"
!#
import scala.collection.JavaConversions._
import java.io._


object EnvironmentVariables extends App {

  // Postgres
  // FileWriter
  val appDir = System.getenv("APP_DIR")
  val file = new File(s"$appDir/build/src/test/resources/postgres_test.properties")
  val bw = new BufferedWriter(new FileWriter(file))

  val dbAddr = System.getenv("POSTGRESQL94_PORT_5432_TCP_ADDR")
  val dbPort = System.getenv("POSTGRESQL94_PORT_5432_TCP_PORT")
  val dbName = System.getenv("POSTGRESQL94_ENV_POSTGRES_DB")
  val dbSchema = System.getenv("POSTGRESQL94_ENV_POSTGRES_SCHEMA_NAME")
  val dbSsl = System.getenv("POSTGRESQL94_ENV_POSTGRES_SSL")
  val dbUserName = System.getenv("POSTGRESQL94_ENV_POSTGRES_USER")
  val dbPassword = System.getenv("POSTGRESQL94_ENV_POSTGRES_PASSWORD")

  bw.write(s"driverClassName=org.postgresql.Driver\n")
  bw.write(s"jdbcUri=dbc:postgresql://$dbAddr:$dbPort/$dbName\n")
  bw.write(s"user=$dbName\n")
  bw.write(s"schema=$dbSchema\n")
  bw.write(s"password=$dbPassword\n")
  bw.write(s"ssl=$dbSsl\n")
  bw.write(s"sslfactory=org.postgresql.ssl.NonValidatingFactory")

  bw.close()

}

EnvironmentVariables.main(args)
