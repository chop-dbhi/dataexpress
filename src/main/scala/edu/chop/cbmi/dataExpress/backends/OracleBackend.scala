package edu.chop.cbmi.dataExpress.backends

import java.util.Properties
import java.io.FileInputStream

/**
 * Backend for accessing Oracle databases. This will use a connection properties file that
 * should look something like the following:
 * {{{driverClassName=jdbc.driver.OracleDriver
 * jdbcUri=jdbc:oracle:thin:@//server_address:server_port
 * user=username
 * schema=schema_name
 * password=password}}}
 * 
 * 
 */

class OracleBackend(override val connectionProperties : Properties, _sqlDialect : SqlDialect = null,
                           _driverClassName : String = null)
  extends SqlBackend(connectionProperties, if(_sqlDialect==null)OracleSqlDialect else _sqlDialect,
    if(_driverClassName==null)"oracle.jdbc.driver.OracleDriver" else _driverClassName) {

    /** If J2EE13Compliant is not set, set it to TRUE, otherwise use the setting that is provided by the user   **/

    if (connectionProperties.getProperty("oracle.jdbc.J2EE13Compliant")  ==  null)  {
      connectionProperties.setProperty("oracle.jdbc.J2EE13Compliant","TRUE")
    }

}
