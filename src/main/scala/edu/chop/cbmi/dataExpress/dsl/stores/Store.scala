package edu.chop.cbmi.dataExpress.dsl.stores
import edu.chop.cbmi.dataExpress.dataModels._
import edu.chop.cbmi.dataExpress.dataWriters.OperationStatus
import beans.BooleanBeanProperty

trait Store {

  def is_closed_? : Boolean

  def open : Boolean

  def close : Boolean

  def save : Boolean

  def createTable(name:String, table:DataTable[_]) : Boolean

  def tableForName(name:String) : DataTable[_]

  def insertRow(tableName : String, row : DataRow[_]) : Boolean

  def insertRow(tableName : String, f : (String)=>Option[_]) : Boolean

  /**
   *
   * @param tableName The name of the table to insert the rows
   * @param table A source table that contains the rows to be inserted
   * @return number of rows successfully inserted
   */
  def insertRows(tableName : String, table : DataTable[_]) : Int

  /**
   * @param updated_row DataRow containing updated values
   * @param filter key,value pairs used to identify the row to update
   */
  def updateRow(table_name : String, updated_row : DataRow[_], filter : (String,_)*) : Boolean

  /**
   * @param filter key,value pairs used to identify the row to update
   * @param f a function that maps the column_names in the table to the new values for the updated row
   */
  def updateRow(table_name : String, filter : (String,_)*)(f:(String)=>Option[_]) : OperationStatus

  def unique_id : Any

  def set_unique_id(id : Any) : Unit
}

