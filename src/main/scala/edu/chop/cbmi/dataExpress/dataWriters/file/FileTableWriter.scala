package edu.chop.cbmi.dataExpress.dataWriters.file

import edu.chop.cbmi.dataExpress.dataWriters.{OperationStatus, DataWriter}
import edu.chop.cbmi.dataExpress.backends.file.{Overwrite, Append, FileBackend}
import edu.chop.cbmi.dataExpress.dataModels.{DataType, DataTable, DataRow}
import scala.collection.mutable.ListBuffer

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/14/13
 * Time: 10:07 AM
 * To change this template use File | Settings | File Templates.
 */

object FileTableWriter{
  val OVERWRITE = 0
  val APPEND = 1
}

/**
 * Note that this class IGNORES table_name IN ALL CALLS
 * @param fb
 */
case class FileTableWriter(fb: FileBackend, column_names: Seq[String]) extends DataWriter{

  def insert_row[T](table_name : String, row : DataRow[T]) : OperationStatus = {
     FileOperationStatus(fb.write(row, Append))
  }

  def insert_row[T](table_name : String, f : (String)=>Option[T]) : OperationStatus = {
    FileOperationStatus(fb.write(DataRow(apply_f_for_cols(f): _*), Append))
  }

  def insert_rows[T](table_name : String, table : DataTable[T]) : OperationStatus = {
    FileOperationStatus(fb.write(table, Append))
  }

  def insert_rows[T](table_name: String, rows : Iterable[DataRow[T]]) : OperationStatus = {
    FileOperationStatus(fb.write(rows.iterator, Append))
  }

  /**
   * @param filter key,value pairs used to identify the row to update
   * @param f a function that maps the column_names in the table to the new values for the updated row
   */
  //def update_row[T](table_name : String, filter : (String,_)*)(f:(String)=>Option[T]) : OperationStatus

  def insert_table[T,G<:DataType](table_name : String, data_types : Seq[G] = Seq.empty[DataType], table : DataTable[T] = DataTable.empty, overwrite_option:Int = -1) : OperationStatus ={
    val writeOption = overwrite_option match{
      case FileTableWriter.OVERWRITE => Overwrite
      case FileTableWriter.APPEND => Append
      case _ => throw new Exception(s"Unsupported overwrite_option = $overwrite_option")
    }
    FileOperationStatus(fb.write(table, writeOption))
  }

  private def apply_f_for_cols[T](f:(String)=>T) = {
    val new_row = ListBuffer.empty[(String,T)]
    column_names.foreach((name:String)=>{
      (f(name) : @unchecked) match{
        case Some(t) => new_row += name->t.asInstanceOf[T]
        case None => {} //row was filtered
      }
    })
    new_row
  }
}
