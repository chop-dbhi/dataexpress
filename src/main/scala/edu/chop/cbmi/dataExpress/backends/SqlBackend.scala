/*
Copyright (c) 2012, The Children's Hospital of Philadelphia All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
   disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
   following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.chop.cbmi.dataExpress.backends

import edu.chop.cbmi.dataExpress.dataModels.DataType
import edu.chop.cbmi.dataExpress.dataModels.{DataTable, DataRow}
import java.util.Properties
import java.io.FileInputStream
import edu.chop.cbmi.dataExpress.logging.Log
import java.sql.{ResultSet, PreparedStatement, Statement, Driver}
import java.util.ServiceLoader

/**
 * Created by IntelliJ IDEA.
 * User: italiam
 * Date: 8/19/11
 * Time: 8:43 AM
 * To change this template use File | Settings | File Templates.
 */

trait SqlBackendProvider {
  def getProviderFor(db_vendor : String, connectionProperties : Properties, sqlDialect : SqlDialect, driverClassName : String) : Option[SqlBackend]
}

object SqlBackendFactory{
	
  val sqlBackendProviderLoader = ServiceLoader.load[SqlBackendProvider](classOf[SqlBackendProvider])
  
  private val included_backends = List("postgresql", "mysql", "sqlite")

  private def load_included_bakcend(db_type: String, connection_properties: Properties, sqlDialect: SqlDialect = null,
    driver_class_name: String = null) = db_type match {
    case "postgresql" => new PostgresBackend(connection_properties, sqlDialect, driver_class_name)
    case "mysql" => new MySqlBackend(connection_properties, sqlDialect, driver_class_name)
    //case "oracle"     => new OracleBackend(connection_properties, sqlDialect, driver_class_name)
    //case "sqlserver"  => new SqlServerBackend(connection_properties, sqlDialect, driver_class_name)
    case "sqlite" => new SqLiteBackend(connection_properties, sqlDialect, driver_class_name)
    case _ => throw new RuntimeException("Unsupported database type: " + db_type)
  }

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

  def apply(connection_properties_file: String, sqlDialect : SqlDialect, driver_class_name : String) : SqlBackend = {
    val prop_stream = new FileInputStream(connection_properties_file)
    val props = new Properties()
    props.load(prop_stream)
    prop_stream.close()
    if (!props.stringPropertyNames().contains("jdbcUri")) {
        throw new RuntimeException("""File %s does not contain required property "jdbcUri""".format(connection_properties_file))
    }
    apply(props, sqlDialect, driver_class_name)
  }

  def apply(connection_properties_file: String, sqlDialect : SqlDialect) : SqlBackend =
    apply(connection_properties_file, sqlDialect, null)

  def apply(connection_properties_file: String, driver_class_name : String) : SqlBackend =
    apply(connection_properties_file, null, driver_class_name)

  def apply(connection_properties_file: String) : SqlBackend = apply(connection_properties_file, null, null)

}

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

  def connect() : java.sql.Connection = connect(connectionProperties)

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

  def get_jdbcUri = if(jdbcUri==null)connectionProperties.getProperty("jdbcUri") else jdbcUri

  def close(connection:java.sql.Connection) = connection.close()

  def close() = {
    if (connection != null) checkResultSetThenExecute {
      connection.close()
      None
    }
  }
  /*------ Query Execution functions -----*/

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
   * @param bindvars set of bind variables for substitution in the statement
   *                 (@see java.sql.PreparedStatement)
   *
   * @return <code>Java.sql.ResultSet</code> representing the query
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

  def commit() : Boolean              = execute(sqlDialect.commit)

  def rollback() : Boolean            = execute(sqlDialect.rollback)

  def startTransaction() : Boolean    = execute(sqlDialect.startTransaction())

  def endTransaction() : Boolean      = execute(sqlDialect.endTransaction())

  /*------ Table Management Methods ------*/

/*  def createTable(tableName: String, colNames:List[String], sqlTypes:List[String]):Boolean = {
    val colTypeMap = colNames.zip(sqlTypes).toMap
    val sql = sqlDialect.createTable(tableName, colTypeMap)
    execute(sql)
  }*/

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
   * @cascade when <code>true</code> indicates that constraints should be cascadedd
   */
  def dropTable(tableName: String, cascade:Boolean=false, schemaName:Option[String] = None) : Boolean =
    execute(sqlDialect.dropTable(tableName, cascade, schemaName))
  /*------ Insertion Methods ------*/

  def insertReturningKeys(tableName: String, row: DataRow[_], schemaName:Option[String] = None): DataRow[_] =
    executeReturningKeys(sqlDialect.insertRecord(tableName, row.column_names.toList, schemaName), row)
 
  def insertRow(tableName: String, row: DataRow[_], schemaName:Option[String] = None): Boolean = 
    execute(sqlDialect.insertRecord(tableName, row.column_names.toList, schemaName), row)
    
  def batchInsert(tableName:String, table:DataTable[_], schemaName:Option[String] = None):Int = {
    val sqlStatement = sqlDialect.insertRecord(tableName, table.column_names.toList, schemaName)
    val statement = statementCache.getStatement(sqlStatement)
    executeBatch(statement, table.iterator, 50, {dr:DataRow[_] => dr})
  }

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
        }
        catch {
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