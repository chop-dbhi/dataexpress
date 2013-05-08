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
import scala.collection.mutable.{ListBuffer}

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

case class ToFromTable(target : Store, source_table : TransformableDataTable) extends To(target) {
  private val CREATE_MODE = 'create
  private val APPEND_MODE = 'append
  private val DEFAULT_BATCH_SIZE = 50
  //private val OVERWRITE_MODE = 'overwrite

  private def writeToSqlTable(table_name: String, s: SqlDb, mode:Symbol) : Boolean = {
    val schema = s.schema
    val catalog = s.catalog.getOrElse(null)
    val col_names = source_table._final_column_names

    mode match{
      case CREATE_MODE => {
        val data_types = source_table._final_data_types match{
           case Some(l:List[DataType]) => l
           case _ => source_table.data_table match {
             case (st: SqlRelation[_]) => st.dataTypes.toList
             case _ => throw new Exception("dataTypes must be provided to create_table for SqlDb and DataTable that is not of type SqlRelation")
           }
         }
        //TODO note the createTable currently forces a drop table
         s.backend.createTable(table_name, col_names, data_types, schema)
         writeToSqlTable(table_name, s, APPEND_MODE)
       }
       case APPEND_MODE => {
         val writer = DataWriter(s.backend, schema, catalog)
         if(source_table._transformers.isEmpty){
           val writer = DataWriter(s.backend, schema, catalog)
           //just insert the table wholesale
           writer.insert_rows(table_name, source_table.data_table).operation_succeeded_?
         }else{
           val buffer = new ListBuffer[DataRow[_]]()

           //fold left over all rows so we can accumulate an AND'ed boolean to know if all batch inserts succeeded
           val status = (true /: source_table.data_table){(b,next_row) =>
              //transform row
             (Some(next_row).asInstanceOf[Option[DataRow[_]]] /: source_table._transformers){ (output, f) =>
               output match{
                 case Some(r:DataRow[_]) => f(r)
                 case _ => None
               }
             }match{
               //add row to buffer if not filtered
               case Some(r) => buffer += r
               case _ => {} //TODO maybe log filtered rows?
             }
             if(buffer.length % DEFAULT_BATCH_SIZE == 0){
               //batch insert the buffer
               val success = writer.insert_rows(table_name, buffer, col_names, schema).operation_succeeded_?
               //TODO if !success log failure, rollback or some such actions
               buffer.clear
               b && success
             }else b
           }//end fold left
           //flush buffer if not empty
           if(buffer.length != 0){
             //batch insert the buffer
             status && (writer.insert_rows(table_name, buffer, col_names, schema).operation_succeeded_?)
           }else status
         }
       }
     }
  }

  def create(table_name : String) = {
    target match {
          case (s:SqlDb) => writeToSqlTable(table_name, s, CREATE_MODE)
          //case (s:ExcelStore) =>
          //case (s:csvStore) =>
          case _ => throw UnsupportedStoreType(target, "create_table")
        }
  }

  //TODO
  def append(table_name : String) = {
    target match{
      case (s: SqlDb) => writeToSqlTable(table_name, s, APPEND_MODE)
      case _ => throw UnsupportedStoreType(target, "append_table")
    }
  }

  //TODO
  //def overwrite(table_name : String)

}