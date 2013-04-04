package edu.chop.cbmi.dataExpress.dataWriters

import edu.chop.cbmi.dataExpress.backends.SqlBackend
import edu.chop.cbmi.dataExpress.dataModels.{DataType, DataTable, DataRow}
import sql.SqlTableWriter

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 12/15/11
 * Time: 1:10 PM
 * To change this template use File | Settings | File Templates.
 */

trait DataWriter {

  def insertRow[T](table_name : String, row : DataRow[T]) : OperationStatus

  def insertRow[T](table_name : String, f : (String)=>Option[T]) : OperationStatus

  def insertRows[T](table_name : String, table : DataTable[T]) : OperationStatus

  /**
   * @param updated_row DataRow containing updated values
   * @param filter key,value pairs used to identify the row to update
   */
  def updateRow[T](table_name : String, updated_row : DataRow[T], filter : (String,_)*) : OperationStatus

  /**
   * @param filter key,value pairs used to identify the row to update
   * @param f a function that maps the column_names in the table to the new values for the updated row
   */
  def updateRow[T](table_name : String, filter : (String,_)*)(f:(String)=>Option[T]) : OperationStatus

  def insertTable[T,G<:DataType](table_name : String, data_types : Seq[G] = Seq.empty[DataType], table : DataTable[T] = DataTable.empty, overwrite_option:Int = -1) : OperationStatus
}

/**Factory method for creating instances of [[edu.chop.cbmi.dataExpress.dataWriters.DataWriter]]*/
object DataWriter{

  def apply(sqlBackend : SqlBackend, schema : Option[String] = None, catalog : String = null)
  = SqlTableWriter(sqlBackend, schema, catalog)

}