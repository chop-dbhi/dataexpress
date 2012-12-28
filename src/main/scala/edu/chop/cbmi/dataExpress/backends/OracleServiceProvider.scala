package edu.chop.cbmi.dataExpress.backends
import java.util.Properties

/**
 * Service provider class for Oracle that allows runtime class loading of the Oracle driver
 */
class OracleServiceProvider extends SqlBackendProvider{
  
  def getProviderFor(db_vendor : String, connectionProperties : Properties, sqlDialect : SqlDialect, driverClassName : String) : Option[SqlBackend] = {
    if(db_vendor == "oracle") Some(new OracleBackend(connectionProperties, sqlDialect, driverClassName))
    else None
  }

}