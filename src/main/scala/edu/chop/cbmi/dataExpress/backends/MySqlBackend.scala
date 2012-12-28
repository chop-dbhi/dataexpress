package edu.chop.cbmi.dataExpress.backends
import edu.chop.cbmi.dataExpress.dataModels.DataRow
import java.util.Properties

/**
 * Backend for accessing MySQL databases. This will use a connection properties file that
 * should look something like the following:
 * {{{driverClassName=com.mysql.jdbc.Driver
 * jdbcUri=jdbc:mysql://server_address:server_port/schema_name
 * user=username
 * password=password
 * jdbcCompliantTruncation=false}}}
 * 
 * 
 */
class MySqlBackend(override val connectionProperties : Properties, _sqlDialect : SqlDialect = null,
                           _driverClassName : String = null)
  extends SqlBackend(connectionProperties, if(_sqlDialect==null)MySqlDialect else _sqlDialect,
    if(_driverClassName==null)"com.mysql.jdbc.Driver" else _driverClassName) {

  /*this is over ridden because MySQL does not support multiple result sets. Thus it is necessary
    to manage result sets to ensure that any open result sets are closed before performing a query.
    The default SqlBackend implementation will manage these when this flag is set to false
  */
  override val SUPPORTS_MULT_RS = false

  /**
   * In order to avoid loading all results of a query into memory, mySQL '''requires''' the 
   * fetch size of a a database query to be -2^31 or Integer.MIN_VALUE, all other values are ignored.
   * 
   * @param fetchSize '''any value supplied will be ignored'''
   * 
   */
  override def executeQuery(sqlStatement: String, bindvars: Seq[Option[_]] = Seq.empty[Option[_]], fetchSize:Int=20): java.sql.ResultSet = {
    super.executeQuery(sqlStatement, bindvars, Integer.MIN_VALUE)
  }
  
}