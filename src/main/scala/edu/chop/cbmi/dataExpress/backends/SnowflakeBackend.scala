package edu.chop.cbmi.dataExpress.backends

import java.util.Properties

/**
 * Backend for accessing Snowflake/Helix databases. This will use a connection properties file that
 * should look something like the following:
 * {{{driverClassName=net.snowflake.client.jdbc.SnowflakeDriver
 * jdbcUri=jdbc:snowflake://site.snowflakecomputing.com:443/...
 * user=username
 * schema=schemaname
 * password=password}}}
 *
 */
class SnowflakeBackend(override val connectionProperties : Properties, _sqlDialect : SqlDialect = null,
                       _driverClassName : String = null)
  extends SqlBackend(connectionProperties, if(_sqlDialect==null)SnowflakeDialect else _sqlDialect,
    if(_driverClassName==null)"net.snowflake.client.jdbc.SnowflakeDriver" else _driverClassName) {


  // unless otherwise specified, assert JSON as the preferred result format -- avoids arrow glitches
  if (connectionProperties.getProperty("JDBC_QUERY_RESULT_FORMAT")  ==  null)  {
    connectionProperties.setProperty("JDBC_QUERY_RESULT_FORMAT", "JSON")
  }
}
