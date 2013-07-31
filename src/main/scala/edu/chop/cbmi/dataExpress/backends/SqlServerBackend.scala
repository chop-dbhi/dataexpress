package edu.chop.cbmi.dataExpress.backends

import java.util.Properties

class SqlServerBackend(override val connectionProperties : Properties, _sqlDialect : SqlDialect = null,
                       _driverClassName : String = null)
  extends SqlBackend(connectionProperties, if(_sqlDialect==null)SqlServerSqlDialect else _sqlDialect,
    if(_driverClassName==null)"com.microsoft.sqlserver.jdbc.SQLServerDriver" else _driverClassName) {

}

