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

  val standardPropsFile = "/props.properties"

  def property(prop: String, path: String = standardPropsFile): String = {
    val is: InputStream = getClass().getResourceAsStream(path);
    val props = new Properties();
    try {
      props.load(is);
      is.close
      props.getProperty(prop);
    } catch {
      case ex: Exception => ""
    }
  }

  def connectToDB(sb : SqlBackend) = {
    sb.connect
    sb
  }

  def mySqlTestDB() = connectToDB(SqlBackendFactory(property("mysql_db_prop_file")))

  def postGresTestDB() = connectToDB(SqlBackendFactory(property("postgres_db_prop_file")))


}