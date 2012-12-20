package edu.chop.cbmi.dataExpress.backends
import java.util.Properties

class OracleServiceProvider extends SqlBackendProvider{
  
  def getProviderFor(db_vendor : String, connectionProperties : Properties, sqlDialect : SqlDialect, driverClassName : String) : Option[SqlBackend] = {
    if(db_vendor == "oracle") Some(new OracleBackend(connectionProperties, sqlDialect, driverClassName))
    else None
  }

}