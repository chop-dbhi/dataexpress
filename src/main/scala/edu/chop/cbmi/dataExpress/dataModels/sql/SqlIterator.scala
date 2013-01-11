package edu.chop.cbmi.dataExpress.dataModels.sql

import java.sql.ResultSet

/**
 * Specialized [[scala.collection.Iterator]] that handles the peculiarities of dealing with a JDBC ResultSet 
 */
abstract class SqlIterator[+T] private[sql](private val sql_query_package: SqlQueryPackage) extends Iterator[T] {
  protected var cursor_advanced = false
  protected var more_rows = false

  lazy protected val result_set: ResultSet = sql_query_package.resultSet
  lazy protected val meta = sql_query_package.meta

  protected def generate_next() : T

  override def hasNext() = {
    if (!cursor_advanced) {
      cursor_advanced = true
      more_rows = result_set.next()
    }
    more_rows
  }

  override def next(): T = {
    if (cursor_advanced) cursor_advanced = false
    else more_rows = result_set.next()
    //Some databases will complain if a result set is not properly closed, need to generate the next set of values
    //then close the result set.
    val next = generate_next()
    if(!hasNext) {
      result_set.close()
    }
    next
  }


  def next_item_in_column(i: Int) = meta.getColumnType(i) match {
    //Postgres Boolean values such as 't' were having issues
    //because they are mapped to java.sql.Types.BIT
    //TODO: this code needs to be moved out of here
    case java.sql.Types.BIT => result_set.getBoolean(i)
    case _ => result_set.getObject(i)
  }
}