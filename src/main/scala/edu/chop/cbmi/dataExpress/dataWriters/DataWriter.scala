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

  def insert_row[T](table_name : String, row : DataRow[T]) : OperationStatus

  def insert_row[T](table_name : String, f : (String)=>Option[T]) : OperationStatus

  def insert_rows[T](table_name : String, table : DataTable[T]) : OperationStatus

  /**
   * @param updated_row DataRow containing updated values
   * @filter key,value pairs used to identify the row to update
   */
  def update_row[T](table_name : String, updated_row : DataRow[T], filter : (String,_)*) : OperationStatus

  /**
   * @param filter key,value pairs used to identify the row to update
   * @param f a function that maps the column_names in the table to the new values for the updated row
   */
  def update_row[T](table_name : String, filter : (String,_)*)(f:(String)=>Option[T]) : OperationStatus

  def insert_table[T,G<:DataType](table_name : String, data_types : Seq[G] = Seq.empty[DataType], table : DataTable[T] = DataTable.empty, overwrite_option:Int = -1) : OperationStatus
}

/**Factory method for creating instances of [[edu.chop.cbmi.dataExpress.dataWriters.DataWriter]]*/
object DataWriter{

  def apply(sqlBackend : SqlBackend, schema : Option[String] = None, catalog : String = null)
  = SqlTableWriter(sqlBackend, schema, catalog)

}