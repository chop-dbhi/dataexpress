package edu.chop.cbmi.dataExpress.backends

import java.util.Properties

/**
 * Service provider class for Oracle that allows runtime class loading of the Oracle driver
 */
class SnowflakeServiceProvider extends SqlBackendProvider{
  
  def getProviderFor(db_vendor : String, connectionProperties : Properties, sqlDialect : SqlDialect, driverClassName : String) : Option[SqlBackend] = {
    if(db_vendor == "snowflake") Some(new SnowflakeBackend(connectionProperties, sqlDialect, driverClassName))
    else None
  }

}