package edu.chop.cbmi.dataExpress.test.util

import java.io.InputStream
import java.util.Properties
import edu.chop.cbmi.dataExpress.backends.{SqlBackendFactory, PostgresBackend, SqlBackend}

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 12/2/11
 * Time: 11:12 AM
 * To change this template use File | Settings | File Templates.
 */

object TestProps {
  def getDbProps(dbName:String) = {
    val filename = choosePropFile(dbName)
	val inputStream = this.getClass().getResourceAsStream(filename)
    val props = new Properties()
    props.load(inputStream)
    props
  }

  def getDbPropFilePath(dbName:String) = { 
   val filename = choosePropFile(dbName)
   this.getClass().getResource(filename).getPath()
 }
  
  private def choosePropFile(dbname: String) = {
    dbname match {
      case "mysql" => "mysql_test.properties"
      case "postgres" => "postgres_test.properties"
      case "sqlite" => "sqlite_test.properties"
      case _ => "%s.properties".format(dbname)
    }
  }

  def connectToDB(sb : SqlBackend) = {
    sb.connect
    sb
  }

  def mySqlTestDB() = connectToDB(SqlBackendFactory(getDbProps("mysql")))
  def postGresTestDB() = connectToDB(SqlBackendFactory(getDbProps("postgres")))


}