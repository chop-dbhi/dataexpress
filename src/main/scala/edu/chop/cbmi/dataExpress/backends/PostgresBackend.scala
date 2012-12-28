package edu.chop.cbmi.dataExpress.backends

import java.util.Properties

/**
 * Backend for accessing Postgres databases. This will use a connection properties file that
 * should look something like the following:
 * {{{driverClassName=org.postgresql.Driver
 * jdbcUri=dbc:postgresql://server_address:server_port/schema_name
 * user=username
 * schema=schemaname
 * password=password}}}
 * 
 * Note that if you are using a self-signed cert for Postgres SSL connection, you may also need 
 * {{{ssl=true
 * sslfactory=org.postgresql.ssl.NonValidatingFactory}}}
 * 
 * The above is less secure, so it's not recommended unless you are operating on your own trusted
 * network.
 * 
 */
class PostgresBackend(override val connectionProperties : Properties, _sqlDialect : SqlDialect = null,
                           _driverClassName : String = null)
  extends SqlBackend(connectionProperties, if(_sqlDialect==null)PostgresSqlDialect else _sqlDialect,
    if(_driverClassName==null)"org.postgresql.Driver" else _driverClassName) {

}
