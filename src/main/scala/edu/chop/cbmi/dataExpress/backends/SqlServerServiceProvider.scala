package edu.chop.cbmi.dataExpress.backends
import  java.util.Properties

class SqlServerServiceProvider extends SqlBackendProvider {
  
  def getProviderFor(db_vendor : String, connectionProperties : Properties, sqlDialect : SqlDialect, driverClassName : String) : Option[SqlBackend] = {
    if(db_vendor == "sqlserver") Some(new SqlServerBackend(connectionProperties, sqlDialect, driverClassName))
    else None
  }

}