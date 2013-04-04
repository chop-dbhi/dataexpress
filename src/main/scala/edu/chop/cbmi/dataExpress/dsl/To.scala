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
package edu.chop.cbmi.dataExpress.dsl

import edu.chop.cbmi.dataExpress.dsl.ETL._
import exceptions.UnsupportedStoreType
import stores.{SqlDb, Store}
import edu.chop.cbmi.dataExpress.dataModels.sql.SqlRelation
import edu.chop.cbmi.dataExpress.dataWriters.DataWriter
import edu.chop.cbmi.dataExpress.dataModels.{DataTable, DataRow, DataType}

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 1/12/12
 * Time: 2:08 PM
 * To change this template use File | Settings | File Templates.
 */

abstract class To(target : Store) {
  def append(table_name : String) : Unit
}

case class ToFromRow(target : Store, row : DataRow[_]) extends To(target) {

  def append(table_name : String) = {
    target match {
      case (s: SqlDb) => {
        val schema = s.schema
        val catalog = s.catalog.getOrElse(null)
        val writer = DataWriter(s.backend, schema, catalog)
        writer.insert_row(table_name, row)
      }
      //case (s:ExcelStore) =>
      //case (s: csvStore) =>
      case _ => throw UnsupportedStoreType(target, "insert_row")

    }
  }
}
//Note that this is causing a compile error because something must be done with all the existing transformed data table
//code. Some of the things from TransformedData table should be brought into the DataExpress data models and SqlTransform
//should be genericized into a transform trait
case class ToFromTable(target: Store, source_table: DataTable[_]) extends To(target) {

  def create(table_name: String) = {
            target.createTable(table_name, source_table)
            target.insertRows(table_name, source_table)
//            s.backend.createTable(table_name, col_names, data_types, schema)
//            val writer = DataWriter(s.backend, schema, catalog)
//            source_table.data_table.foreach((next_row:DataRow[_])=>{
//              val row_to_insert =
//               (Some(next_row).asInstanceOf[Option[DataRow[_]]] /: source_table._transformers)((output:Option[DataRow[_]], f:(DataRow[_])=>Option[DataRow[_]])=>{
//                 output match{
//                   case Some(r:DataRow[_]) => f(r)
//                   case _ => None
//                 }
//
//               })
//              (row_to_insert : @unchecked) match{
//                case Some(r:DataRow[_]) => writer.insert_row(table_name, r)
//                case None =>  {} //TODO maybe do some logging here of filtered rows?
//              }
//
//            })
  }

  //TODO
  def append(table_name : String) = throw new Exception("not implemented yet")

  //TODO
  //def overwrite(table_name : String)

}