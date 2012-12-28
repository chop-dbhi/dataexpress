package edu.chop.cbmi.dataExpress.backends
import edu.chop.cbmi.dataExpress.dataModels.DataRow
import java.util.Properties
import java.sql.PreparedStatement

/**
 * Backend for accessing Sqlite databases. This will use a connection properties file that
 * should look something like the following:
 * {{{jdbcUri=jdbc:sqlite:filename_of_the_sqlite_database}}}
 * 
 */
class SqLiteBackend(override val connectionProperties : Properties, _sqlDialect : SqlDialect = null, _driverClassName : String = null)
  extends SqlBackend(connectionProperties, if(_sqlDialect==null)SqLiteDialect else _sqlDialect,
    if(_driverClassName==null)"org.sqlite.JDBC" else _driverClassName) {

  /**
   * SQLite does not support returning auto-generated keys, so calls to this method will call a normal
   * execute and will always return an empty data row
   */
  override def executeReturningKeys(sqlStatement:String, bindVars: Seq[Option[_]]): DataRow[_] = {
    super.execute(sqlStatement, bindVars)
    //TODO: when logging is added, include a warning wehenver anyone calls this method
    DataRow.empty
  }
  

//  override val SUPPORTS_MULT_RS = false

  override protected def prepStatement(sqlStatement: PreparedStatement, bindVars: Seq[Option[_]]) = {
    if (bindVars.length > 0) {
      val vars = bindVars.zipWithIndex
      for (v <- vars) {
        //TODO: Too many edge cases in here, need to explicity set some more date/time stuff
        v._1 match {
          case Some(i: java.sql.Timestamp)    => sqlStatement.setString((v._2 + 1), i.toString())
          case Some(i: java.sql.Time)         => sqlStatement.setString((v._2 + 1), i.toString())
          case Some(i: java.sql.Date)		  => sqlStatement.setString((v._2 + 1), (new java.text.SimpleDateFormat("yyyy-MM-dd")).format(i))
          //TODO: Test the java.util.Date for precision here to avoid trying to set to a higher precision
          case Some(i: java.util.Date)        => sqlStatement.setString((v._2 + 1), java.text.DateFormat.getDateTimeInstance().format(i))
          case None => sqlStatement.setNull(v._2 + 1, java.sql.Types.NULL)  //TODO: do something better here
          case _    => sqlStatement.setObject((v._2 + 1), v._1.get)
        }
      }
    }
  }
}
