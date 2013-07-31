package edu.chop.cbmi.dataExpress.backends

import java.util.Properties

/**
 * Backend for accessing SqlServer databases. This will use a connection properties file that
 * should look something like the following:
 * {{{driverClassName=com.microsoft.sqlserver.jdbc.SQLServerDriver
jdbcUri=jdbc:sqlserver://server_address:server_port
database=database_name
user=user_name
password=password
}}}
 *
 */


class SqlServerBackend(override val connectionProperties : Properties, _sqlDialect : SqlDialect = null,
                       _driverClassName : String = null)
  extends SqlBackend(connectionProperties, if(_sqlDialect==null)SqlServerSqlDialect else _sqlDialect,
    if(_driverClassName==null)"com.microsoft.sqlserver.jdbc.SQLServerDriver" else _driverClassName) {

}

