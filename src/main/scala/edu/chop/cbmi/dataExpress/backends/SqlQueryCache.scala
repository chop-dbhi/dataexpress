package edu.chop.cbmi.dataExpress.backends
import java.sql.Statement


/** Factory for [[edu.chop.cbmi.dataExpress.backends.SqlQueryCache]] instances */
object SqlQueryCache {
  /**
   * Creates a new [[edu.chop.cbmi.dataExpress.backends.SqlQueryCache]]
   * 
   * @param size the number of statements to hold in the cache
   * @param connection the JDBC connection for which the cache should be used 
   */
  def apply(size: Int, connection:java.sql.Connection) = {
    new SqlQueryCache(size,connection)
  }
}

/**
 * 
 * While the JDBC spec encourages individual drivers to handle their own query cache, 
 * this behavior is not guaranteed. This class exists to provide consistent cache functionality
 * across all drivers in DataExpress
 * 
 */
class SqlQueryCache(size: Int, connection:java.sql.Connection) {
  /** Map of string statement values to corresponding prepared statements */
  val statementMap = new scala.collection.mutable.LinkedHashMap[String,java.sql.PreparedStatement]()
  /** Statement mapping for statements expected to return primary keys */
  val statementsWithKeysMap = new scala.collection.mutable.LinkedHashMap[String,java.sql.PreparedStatement]()
  
  def prepReturningKeys(sql:String) = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
  def prepSimpleStatement(sql:String) = connection.prepareStatement(sql)
  
  /**
   * Prepare a SQL statement by first checking the cache to see if it exists. If the exact SQL string
   * does not exist in the cache, the driver will be asked to prepare the statement. The resulting statement
   * is added to the cache and returned.
   * 
   * @param sql A SQL string of the statement to be prepared
   * @param returnKeys Set to {{{true}}} if auto-generated primary keys are to be returned
   */
  private def prepStatement(sql: String, returnKeys: Boolean) = {
    val (prepareMethod, cache) = returnKeys match {
      case true => (prepReturningKeys(sql), statementsWithKeysMap)
      case false => (prepSimpleStatement(sql), statementMap)
    }

    val preparedStatement = cache.getOrElseUpdate(sql, prepareMethod)

    if (cache.size > size) {
      cache.head._2.close()
      cache.remove(cache.head._1)
    }

    preparedStatement
  }
  
  /** Retrieve an ordinary statement from the cache (if it does not exist, it will be created) */
  def getStatement(sql: String): java.sql.PreparedStatement = {
     prepStatement(sql, false)
  }
  
  /** Retrieve a statement that returns auto-generated primary keys from the cache 
   * (if it does not exist, it will be created) */
  def getStatementReturningKeys(sql: String): java.sql.PreparedStatement = {
    prepStatement(sql, true)
  }
  
  /** Cleans up the cache (usually in preparation for closing connections and committing results) 
   * by closing all statements. 
   */
  def cleanUp() = {
    List(statementMap, statementsWithKeysMap).map{closeAndClear(_)}
  }
  
  private def closeAndClear(map:scala.collection.mutable.LinkedHashMap[String,java.sql.PreparedStatement]) = {
    statementMap.foreach{e => 
      /* Annoyingly, some JDBC drivers don't implement .isClosed(), so this try/catch guards against a 
       * runtime error and just forces the statement closed if the exception gets thrown.
       */
      try{
	    if (!e._2.isClosed()) {
	      e._2.close() 
	    }
	  }
      catch {
        case ex:java.lang.AbstractMethodError => e._2.close()
      }
	    statementMap.remove(e._1)
	  }
  }
}