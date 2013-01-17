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
package edu.chop.cbmi.dataExpress.dataWriters.sql

import edu.chop.cbmi.dataExpress.backends.SqlBackend
import edu.chop.cbmi.dataExpress.exceptions.TableDoesNotExist
import collection.mutable.ListBuffer
import edu.chop.cbmi.dataExpress.dataModels.{DataType, DataTable, DataRow}
import edu.chop.cbmi.dataExpress.dataWriters.{Updater, DataWriter}

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 12/16/11
 * Time: 12:37 PM
 * To change this template use File | Settings | File Templates.
 */

object SqlTableWriter{
  val OVERWRITE_OPTION_DROP = 0
  val OVERWRITE_OPTION_TRUNCATE = 1
  val OVERWRITE_OPTION_APPEND = 2
}

case class SqlTableWriter(val backend : SqlBackend, val schema : Option[String] = None, val catalog : String = null)
  extends DataWriter with Updater{

  private def column_names(table_name : String) = {
    val rs = backend.connection.getMetaData.getColumns(catalog, schema.getOrElse(null), table_name, null)
    var names =  scala.collection.mutable.HashMap.empty[Int,String]
    if(rs.next){
      do{
        names += (rs.getInt(17)->rs.getString(4))
      }while(rs.next)
      names.toList.sortBy(_._1).map(_._2)
    }else throw TableDoesNotExist(table_name)
  }

  /**
   * @param row A DataRow whose column names match columns in the target table, not all columns are required
   * @return SqlOperationsStatus contains status and primary key of newly inserted row
   */
  override def insert_row[T](table_name:String, row: DataRow[T]) =
    SqlOperationStatus(true, backend.insertReturningKeys(table_name, row, schema))

  /**
   * @param table A DataTable whose column names match columns in the target table, not all columns are required
   * @return SqlOperationsStatus contains status and primary keys for each row
   */
  override def insert_rows[T](table_name: String, table: DataTable[T]) = {
    //TODO for logging it would help to know how many rows were inserted
    val result = backend.batchInsert(table_name, table, schema)
    SqlOperationStatus(true)
  }

  override def insert_rows[T](table_name: String, rows: Iterable[DataRow[T]]) = {
    val result = backend.batchInsertRows(table_name, rows.iterator, column_names(table_name), schema)
    if(result == -1)SqlOperationStatus(false) else SqlOperationStatus(true)
  }


  /**
   * @param f A function that takes a column_name : String and returns the value for that column
   */
  override  def insert_row[T](table_name:String, f : (String)=>Option[T]) =
    SqlOperationStatus(true, backend.insertReturningKeys(table_name, DataRow(apply_f_for_cols(table_name,f): _*), schema))

  override def update_row[T](table_name : String, updated_row : DataRow[T], filter : (String,_)*) =
    SqlOperationStatus(backend.updateRow(table_name, updated_row, filter.toList, schema))

  def update_row[T](table_name : String, filter : (String,_)*)(f:(String)=>Option[T]) =
    SqlOperationStatus(backend.updateRow(table_name, DataRow(apply_f_for_cols(table_name, f):_*), filter.toList, schema))

  private def apply_f_for_cols[T](table_name:String, f:(String)=>T) = {
    val new_row = ListBuffer.empty[(String,T)]
    column_names(table_name).foreach((name:String)=>{
      (f(name) : @unchecked) match{
        case Some(t) => new_row += name->t.asInstanceOf[T]
        case None => {} //row was filtered
      }
    })
    new_row
  }

  override def insert_table[T,G<:DataType](table_name: String, data_types: Seq[G] = Seq.empty[DataType], table: DataTable[T],
                                           overwrite_option: Int = SqlTableWriter.OVERWRITE_OPTION_APPEND) = {
    if(data_types.isEmpty)throw new Exception("data_types list is empty in insert table")
    else {
      overwrite_option match {
        case SqlTableWriter.OVERWRITE_OPTION_DROP =>
          backend.createTable(table_name, table.columnNames.toList, data_types.toList, schema)
        case SqlTableWriter.OVERWRITE_OPTION_TRUNCATE => backend.truncateTable(table_name)
        case SqlTableWriter.OVERWRITE_OPTION_APPEND =>{} //nothing to do hear
        case _ => throw new Exception("Unsupported option : " + overwrite_option)
      }
      if(table != DataTable.empty) insert_rows(table_name, table)
      else SqlOperationStatus(true)
    }
  }


}