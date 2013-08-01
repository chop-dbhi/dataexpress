package edu.chop.cbmi.dataExpress.test.util

import java.io.InputStream
import scala.io.Source
import java.util.Properties
import edu.chop.cbmi.dataExpress.backends.{SqlBackendFactory, PostgresBackend, SqlBackend}

object TestProps {
  def getDbProps(dbName:String):Properties = {
    val filename = choosePropFile(dbName)
	val inputStream = getClass.getResourceAsStream(filename)
    //val inputStream = this.getClass().getResourceAsStream(filename)
	val props = new Properties()
    props.load(inputStream)
    inputStream.close()
    props
  }

  def getDbPropFilePath(dbName:String) = { 
   val filename = choosePropFile(dbName)
   getClass().getResource(filename).getPath()
 }
  
  private def choosePropFile(dbname: String) = {
    val travis = System.getenv("TRAVIS")

    travis match {
      case "true"=> getTravisDbProps(dbname)
      case _ => getLocalDbProps(dbname)
    }

  }

  private def getTravisDbProps(dbname: String) = {
    dbname match {
      case "mysql" => "/mysql_travis_test.properties"
      case "postgres" => "/postgres_travis_test.properties"
      case "sqlite" => "/sqlite_test.properties"
      case _ => "/%s_travis_test.properties".format(dbname)

    }
  }
  private def getLocalDbProps(dbname: String) = {
    dbname match {
      case "mysql" => "/mysql_test.properties"
      case "postgres" => "/postgres_test.properties"
      case "sqlite" => "/sqlite_test.properties"
      case "sqlserver" => "/sqlserver_test.properties"
      case _ => "/%s_test.properties".format(dbname)
    }

  }


  def connectToDB(sb : SqlBackend) = {
    sb.connect
    sb
  }

  def mySqlTestDB() = connectToDB(SqlBackendFactory(getDbProps("mysql")))
  def postGresTestDB() = connectToDB(SqlBackendFactory(getDbProps("postgres")))


}