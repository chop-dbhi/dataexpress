package edu.chop.cbmi.dataExpress.backends

import java.util.Properties
import java.io.FileInputStream

/**
 * Created by IntelliJ IDEA.
 * User: italiam
 * Date: 8/19/11
 * Time: 9:17 AM
 * To change this template use File | Settings | File Templates.
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
