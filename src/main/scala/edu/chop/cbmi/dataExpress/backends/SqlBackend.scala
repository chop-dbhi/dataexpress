package edu.chop.cbmi.dataExpress.backends

import edu.chop.cbmi.dataExpress.dataModels.DataType
import edu.chop.cbmi.dataExpress.dataModels.{DataTable, DataRow}
import java.util.Properties
import java.io.FileInputStream
import edu.chop.cbmi.dataExpress.logging.Log
import java.sql.{ResultSet, PreparedStatement, Statement, Driver}
import java.util.ServiceLoader

/** 
 * Provides a mechanism to get a SqlBackend by supplying the necessary parameters at runtime
 * 
 * @param db_vendor A string value for the database vendor (e.g. postgresql, mysql, sqlite, oracle, etc...)
 * @param connectionProperties a [[java.util.Properties]] object that contains the connection information
 * @param sqlDialect an [[edu.chop.cbmi.dataExpress.backends.SqlDialect]] to be used with the database system
 * @param driverClassName the string name of the JDBC driver class
 * 
 */
trait SqlBackendProvider {
  def getProviderFor(db_vendor : String, connectionProperties : Properties, sqlDialect : SqlDialect, driverClassName : String) : Option[SqlBackend]
}

/** Factory for [[edu.chop.cbmi.dataExpress.backends.SqlBackendFactory]] */
object SqlBackendFactory{
	
  val sqlBackendProviderLoader = ServiceLoader.load[SqlBackendProvider](classOf[SqlBackendProvider])
  private val included_backends = List("postgresql", "mysql", "sqlite", "oracle")
  private val included_backends = List("postgresql", "mysql", "sqlite", "oracle", "sqlserver")

  private def load_included_bakcend(db_type: String, connection_properties: Properties, sqlDialect: SqlDialect = null,
    driver_class_name: String = null) = db_type match {
    case "postgresql" => new PostgresBackend(connection_properties, sqlDialect, driver_class_name)
    case "mysql" => new MySqlBackend(connection_properties, sqlDialect, driver_class_name)
    case "oracle"     => new OracleBackend(connection_properties, sqlDialect, driver_class_name)
    //case "sqlserver"  => new SqlServerBackend(connection_properties, sqlDialect, driver_class_name)
    case "sqlite" => new SqLiteBackend(connection_properties, sqlDialect, driver_class_name)
    case "sqlserver" => new SqlServerBackend(connection_properties, sqlDialect, driver_class_name)
    case _ => throw new RuntimeException("Unsupported database type: " + db_type)
  }
  
/**
 * Creates the appropriate [[edu.chop.cbmi.dataExpress.backends.SqlBackend]] from a [[java.util.Properties]] object
 * 
 * @param connection_properties a [[java.util.Properties]] object containing all the necessary information to connect
 * @param sqlDialect The [[edu.chop.cbmi.dataExpress.backends.SqlDialect]] to use with the database
 * @param driver_class_name The string name of the driver class to be used
 */
  def apply(connection_properties: Properties, sqlDialect: SqlDialect = null,
    driver_class_name: String = null): SqlBackend = {
    // try {
    val db_type: String = connection_properties.getProperty("jdbcUri").split(":")(1)
    if (included_backends contains db_type) load_included_bakcend(db_type, connection_properties, sqlDialect, driver_class_name)
    else{
      val providers = sqlBackendProviderLoader.iterator()
      var provider : SqlBackend = null
      var keep_searching = true
      while(providers.hasNext() && keep_searching){
        providers.next().getProviderFor(db_type, connection_properties, sqlDialect, driver_class_name) match{
          case Some(p) => {
            provider = p
            keep_searching = false
          }
          case _ => keep_searching = true
        }
      }
      provider
    }
    //     } catch {
    //       case _ => throw new RuntimeException("Required property 'jdbcUri' not in properties file")
    //     }
  }
  
  /**
   * Creates the appropriate [[edu.chop.cbmi.dataExpress.backends.SqlBackend]] from a .properties file
   *
   * @param connection_properties a .properties file that can be serialized to a [[java.util.Properties]] object containing all the necessary information to connect
   * @param sqlDialect The [[edu.chop.cbmi.dataExpress.backends.SqlDialect]] to use with the database
   * @param driver_class_name The string name of the driver class to be used
   */
  def apply(connection_properties_file: String, sqlDialect: SqlDialect, driver_class_name: String): SqlBackend = {
    val prop_stream = new FileInputStream(connection_properties_file)
    val props = new Properties()
    props.load(prop_stream)
    prop_stream.close()
    if (!props.stringPropertyNames().contains("jdbcUri")) {
        throw new RuntimeException("""File %s does not contain required property "jdbcUri""".format(connection_properties_file))
    }
    apply(props, sqlDialect, driver_class_name)
  }
  
  /**
   * Creates the appropriate [[edu.chop.cbmi.dataExpress.backends.SqlBackend]] from a .properties file
   *
   * @param connection_properties a .properties file that can be serialized to a [[java.util.Properties]] object containing all the necessary information to connect
   * @param sqlDialect The [[edu.chop.cbmi.dataExpress.backends.SqlDialect]] to use with the database
   */
  def apply(connection_properties_file: String, sqlDialect : SqlDialect) : SqlBackend =
    apply(connection_properties_file, sqlDialect, null)

  /**
   * Creates the appropriate [[edu.chop.cbmi.dataExpress.backends.SqlBackend]] from a .properties file
   *
   * @param connection_properties a .properties file that can be serialized to a [[java.util.Properties]] object containing all the necessary information to connect
   * @param driver_class_name The string name of the driver class to be used
   */
  def apply(connection_properties_file: String, driver_class_name : String) : SqlBackend =
    apply(connection_properties_file, null, driver_class_name)

  /**
   * Creates the appropriate [[edu.chop.cbmi.dataExpress.backends.SqlBackend]] from a .properties file
   *
   * @param connection_properties a .properties file that can be serialized to a [[java.util.Properties]] object containing all the necessary information to connect
   * @param driver_class_name The string name of the driver class to be used
   */
  def apply(connection_properties_file: String) : SqlBackend = apply(connection_properties_file, null, null)

}
/**
 * Wrapper around JDBC that simplifies the mechanics of interacting with databases in an RDBMS-neutral way.
 * 
 * Instances of SqlBacked should normally be instantiated via [[edu.chop.cbmi.dataExpress.backends.SqlBackendFactory]]
 */
case class  SqlBackend(connectionProperties : Properties, sqlDialect : SqlDialect, driverClassName : String) {
  var connection:java.sql.Connection = _
  var statementCache:SqlQueryCache = _
  val CACHESIZE=20
  private var jdbcUri:String = _
  private var openResultSet : Option[ResultSet] = None

  /*this flag indicates if the backend supports multiple result sets. If set to false, the backend attempts to
  track open result sets and close them before executing a new query*/
  val SUPPORTS_MULT_RS = true

  /*------ Connection Management functions ------*/
  /**
   * Opens the connection to the database
   */
  def connect() : java.sql.Connection = connect(connectionProperties)

  /**
   * Opens a connection to the database via properties defined in a [[java.util.Properties]] object
   * 
   * @param props the [[java.util.Properties]] object with the connect information
   */
  def connect(props:Properties):java.sql.Connection = {

    val dr = java.lang.Class.forName(driverClassName).newInstance().asInstanceOf[Driver]

    if (props.stringPropertyNames().contains("jdbcUri")) {
      jdbcUri = props.getProperty("jdbcUri")
      val connectProps = new Properties(props)
      connectProps.remove("jdbcUri")
      connection = dr.connect(jdbcUri, connectProps)
      connection.setAutoCommit(false)
      statementCache = new SqlQueryCache(CACHESIZE, connection)
      connection
    }
    else {
      throw new RuntimeException("Required Property 'jdbcUri' not present. Properties are: %s".format(props.stringPropertyNames()))
    }
  }
  
  /** returns the URI for the JDBC connection
   */
  //TODO: Check to confirm this is actually still needed
  def get_jdbcUri = if(jdbcUri==null)connectionProperties.getProperty("jdbcUri") else jdbcUri

  /** Closes a [[java.sql.Connection]] 
   * @param connection the connection to close
   */
  def close(connection:java.sql.Connection) = connection.close()

  /** Closes the [[java.sql.connection]] associated with instances of the backend */
  def close() = {
    if (connection != null) checkResultSetThenExecute {
      statementCache.cleanUp
      connection.close()
      if (!connection.isClosed()) {
        throw new java.lang.RuntimeException("Failed to close database connection")
      }
      None
    }
  }
  /*------ Query Execution functions -----*/
/**
 * Finesses the situation where some databases allow for only one open result set at a time. This is called
 * whenever a new result set might be created to ensure that any open result set is closed. 
 * 
 * @param code a code block that is likely to return a ResultSet
 * 
 */
 protected def checkResultSetThenExecute(code: => Option[ResultSet]) = {
    if(SUPPORTS_MULT_RS)code
    else{
      openResultSet match {
        case None => {}
        case Some(ors) => ors.close
      }
      val rs = code
      openResultSet = rs
      rs
    }
  }

  /**
   * Will execute a SQL SELECT statement with a fetch size of 20.
   *
   * @param sqlStatement the <code>SELECT</code> statement to run
   *
   * @param bindvars set of bind variables for substitution in the statement (see [[java.sql.PreparedStatement]])
   *
   * @return [[java.sql.ResultSet]] representing the query
   *
   */
  def executeQuery(sqlStatement: String, bindvars: Seq[Option[_]] = Seq.empty[Option[_]], fetchSize:Int=20): java.sql.ResultSet = {
    checkResultSetThenExecute{
      val statement = statementCache.getStatement(sqlStatement)
      prepStatement(statement, bindvars)
      //ideally, you'd like to set this based on how much memory a row
      //unfortunately, the JVM can't tell you how much memory something uses
      //TODO: dynamically calculate a good fetch size during activity
      statement.setFetchSize(fetchSize)
      Some(statement.executeQuery)
    } match {
      case Some(rs) =>rs
      case _ => null
    }
  }

  /**
   * Runs <code>UPDATE</code>, <code>INSERT</code>, or <code>DELETE</code>
   * statement.
   * @param sqlStatement the statement to run
   * @param bindVars set of bind variables to use
   *
   * @return true if no errors were encountered
   */
   def execute(sqlStatement: String, bindVars: Seq[Option[_]] = Seq.empty[Option[_]]) : Boolean = {
     val statement = statementCache.getStatement(sqlStatement)
     prepStatement(statement, bindVars)
     var isFirstResultAResultSet = false
     checkResultSetThenExecute{
       isFirstResultAResultSet = statement.execute()
       if(isFirstResultAResultSet)Some(statement.getResultSet)
       else None
     }
     isFirstResultAResultSet
   }

   /**
    * Executes a SQL statement where new keys are auto-generated from an auto-incrementing primary key.
    * Autogenerated keys are returned as a result of the statement. Not all databases implement this, and their
    * individual implementations vary widely
    * 
    * @param sqlStatement the statement to be run 
    * @param bindVars set of bind variables to use
    */
   def executeReturningKeys(sqlStatement: String, bindVars: Seq[Option[_]]): DataRow[_] = {
     val statement = statementCache.getStatementReturningKeys(sqlStatement)
     prepStatement(statement, bindVars)
     checkResultSetThenExecute{
       val isResultSet = statement.execute()
       if(isResultSet)Some(statement.getResultSet)
       else None
     }
     val keyResultSet = checkResultSetThenExecute {
       Some(statement.getGeneratedKeys)
     }match {
       case Some(rs) =>rs
       case _ => null
     }
     val columnNumber = keyResultSet.getMetaData.getColumnCount
     if (keyResultSet.next) {
       val data = (1 to columnNumber).map{x => keyResultSet.getObject(x)}.toList
       val columns = (1 to columnNumber).map{i => keyResultSet.getMetaData.getColumnLabel(i)}.toList
       DataRow(columns)(DataRow.map_to_option(data))
     }else DataRow.empty

  }
   
  /** 
   * Commit any open transactions to the database
   */
  def commit(): Boolean = execute(sqlDialect.commit)
  
  /**
   * Rollback an existing transaction
   */
  def rollback(): Boolean = execute(sqlDialect.rollback)

  /**
   * Start a new transaction (potentially closing the old one)
   */
  def startTransaction(): Boolean = execute(sqlDialect.startTransaction())
  
  /**
   * End the existing transaction
   */
  def endTransaction(): Boolean = execute(sqlDialect.endTransaction())

  /*------ Table Management Methods ------*/
  
  /**
   * Create a new table
   * 
   * @param tableName the name of the table
   * @param columnNames the list of column names to use
   * @param dataTypes a list of DataExpress [[edu.chop.cbmi.dataExpress.dataModels.DataType]] objects that correspond to the columns
   * @param schemaNames The name of the schema where the table will be created
   */
  def createTable(tableName: String, columnNames : List[String], dataTypes: List[DataType], schemaName:Option[String] = None) :
  Boolean = {
    //Initially, this seems wasteful to get tables each time, but there's no way
    //to know if a table has been created immediately before this and a real risk
    //of a stale cache, also need to account for case-sensitive RDBMS
    val rs = connection.getMetaData.getTables(null, schemaName.getOrElse(null), "%", null)
    while(rs.next) {
      val tableFromMeta = rs.getString(3)
      if (tableFromMeta.toUpperCase == tableName.toUpperCase){
        //automatically cascades constraints, this is usually what you want with ETL
        //TODO: this side effect should probably be removed and pushed up to the DSL or some higher level place
        dropTable(tableName, true, schemaName)
      }

    }
    val typeMap = columnNames.zip(dataTypes).toList
    execute(sqlDialect.createTable(tableName, typeMap, schemaName))

  }

  /**
   *  <code>Truncate</code>s a SQL table
   *  @param tableName the name of the table to truncate
   */
  def truncateTable(tableName: String, schemaName:Option[String] = None) : Boolean = execute(sqlDialect.truncate(tableName, schemaName))

  /**
   * Drops a database table, optionally cascading constraints
   *
   * @param tableName the name of the table to drop
   * @param cascade when {{{true}}} indicates that constraints should be cascadedd
   */
  def dropTable(tableName: String, cascade:Boolean=false, schemaName:Option[String] = None) : Boolean =
    execute(sqlDialect.dropTable(tableName, cascade, schemaName))
  /*------ Insertion Methods ------*/
  /** 
   * Insert a single [[edu.chop.cbmi.dataModels.DataRow]] into a table, returning auto-generated primary keys
   * 
   * 
   * @param tableName The name of the table to do the insert
   * @param row The [[edu.chop.cbmi.dataExpress.dataModels.DataRow]] to insert
   * @param schemaName The schema where the table is located
   */
  def insertReturningKeys(tableName: String, row: DataRow[_], schemaName:Option[String] = None): DataRow[_] =
    executeReturningKeys(sqlDialect.insertRecord(tableName, row.column_names.toList, schemaName), row)
 
   /**
   * Insert a single [[edu.chop.cbmi.dataExpress.dataModels.DataRow]] into a table
   * 
   * @param tableName The name of the table to do the insert
   * @param row The [[edu.chop.cbmi.dataExpress.dataModels.DataRow]] to insert
   * @param schemaName The schema where the table is located
   */
  def insertRow(tableName: String, row: DataRow[_], schemaName:Option[String] = None): Boolean = 
    execute(sqlDialect.insertRecord(tableName, row.column_names.toList, schemaName), row)
  
  /**
   * Perform a batch insert into a table. '''This is the preferred insertion method for large
   * insert operations'''
   * 
   * @param tableName The name of the table to do the insert
   * @param table A [[edu.chop.cbmi.dataExpress.dataModels.DataTable]] that holds the data for the insert
   * @param schemaName The schema where the table is located
   */
  def batchInsert(tableName:String, table:DataTable[_], schemaName:Option[String] = None):Int = {
    val sqlStatement = sqlDialect.insertRecord(tableName, table.column_names.toList, schemaName)
    val statement = statementCache.getStatement(sqlStatement)
    executeBatch(statement, table, 50, {dr:DataRow[_] => dr})
  }

  /**
   * Perform a batch insert into a table. '''This is the preferred insertion method for large
   * insert operations'''
   *
   * @param tableName The name of the table to do the insert
   * @param rows An interable over [[edu.chop.cbmi.dataExpress.dataModels.DataRow]] that holds the data for the insert
   * @param columnNames List[String] of the column names
   * @param schemaName The schema where the table is located
   * Assumes all rows have the same length and column names
   */
  def batchInsertRows(tableName:String, rows:Iterator[DataRow[_]], columnNames: List[String], schemaName:Option[String] = None):Int = {
    if(!rows.isEmpty){
      val sqlStatement = sqlDialect.insertRecord(tableName, columnNames, schemaName)
      val statement = statementCache.getStatement(sqlStatement)
      executeBatch(statement, rows, 50, {dr:DataRow[_] => dr})
    }else -1
  }

  /**
   * Update existing table rows. In order to allow specificity,
   * the <code>filter</code> parameter is a set of (`columnName`, `value`) tuples. These tuples
   *  get converted into the `WHERE` clause. For example:
   *
   *  {{{List(("id",12345),("type","Luggage Combination"))}}}
   *
   *  passed in as a filter is converted to the SQL `WHERE id = 12345 AND type = 'Luggage Combination' Currently,
   *  there is no support for operations other than equality on filters (e.g. ```WHERE id > 100```).
   * 
   * @param tableName The name of the table to do the insert
   * @param updated_row The contents to be used for the update
   * @param filter A list of (`columnName`,`value`) tuples to be used when constructing the `WHERE` clause
   * @param schemaName The schema where the table is located  
   */
  def updateRow(tableName: String, updated_row: DataRow[_], filter: List[(String, Any)], schemaName: Option[String] = None) = {
    val sqlStatement = sqlDialect.updateRecords(tableName, updated_row.column_names.toList, filter, schemaName)
    val bind_vars = DataRow.map_to_option(updated_row.map((v:Option[_])=>{
      v match {
        case Some(s) => s
        case None => null
      }
    }).toList ::: filter.map(_._2).toList)

    execute(sqlStatement, bind_vars)
  }
  
  /**
   * Executes a set of [[java.sql.PreparedStatement]]s in a batch mode. The primary operation where this makes
   * sense is ```INSERT``` operations, but one could imagine a stored procedure used in this way as well
   * 
   * @param statement The [[java.sql.PreparedStatement]] to be used for the insert
   * @param values The bind values of any bind variables that might be needed for placeholders in the prepared statement
   * @param batchSize The size of the batch to use before executing the batch in the database
   * @param callback A function that will be applied to each set of data values before they are added to the prepared statement
   * 
   * @return the number of statements that correctly executed
   */
  def executeBatch[T](statement: java.sql.PreparedStatement,
                      values: Iterator[T], batchSize: Int, callback: T => Seq[Option[_]]): Int = {

    var currentBatch = 0
    var successfulStatementCount = 0

    while (values.hasNext) {
      val bindVars = callback(values.next())
      prepStatement(statement, bindVars)
      statement.addBatch()
      currentBatch += 1
      if (currentBatch % batchSize == 0) {
        //TODO: Some transaction handling to bail out if we fail
        try {
          val status = statement.executeBatch.toList
          successfulStatementCount += status.filter(i => i != Statement.EXECUTE_FAILED).length
        } catch {
          case e: java.sql.BatchUpdateException => {
            throw e.getNextException
          }
        }
      }
    }
    if (currentBatch % batchSize != 0) {
      val status = statement.executeBatch.toList
      successfulStatementCount += status.filter(i => i != Statement.EXECUTE_FAILED).length
    }
    successfulStatementCount
  }
 
  /**
   * Generate a JDBC [[java.sql.PreparedStatement]] using bindVariables
   * 
   * @param sqlStatement A [[java.sql.PreparedStatement]]  that has place holders for bind variables
   * @param bindVars A list of values to be bound to the statement
   */
  protected def prepStatement(sqlStatement: PreparedStatement, bindVars: Seq[Option[_]]) = {
    if (bindVars.length > 0) {
      val vars = bindVars.zipWithIndex
      for (v <- vars) {
        //TODO: Too many edge cases in here, need to explicity set some more date/time stuff
        v._1 match {
          case Some(i: java.sql.Timestamp)    => sqlStatement.setTimestamp((v._2 + 1), i)
          case Some(i: java.sql.Time)         => sqlStatement.setTime((v._2 + 1), i)
          case Some(i: java.sql.Date)		  => sqlStatement.setDate((v._2 + 1), i)
          //TODO: Test the java.util.Date for precision here to avoid trying to set to a higher precision
          case Some(i: java.util.Date)        => sqlStatement.setDate((v._2 + 1), new java.sql.Date(i.getTime))
          case None => sqlStatement.setNull(v._2 + 1, java.sql.Types.NULL)  //TODO: do something better here
          case _    => sqlStatement.setObject((v._2 + 1), v._1.get)
        }
      }
    }
  }
}