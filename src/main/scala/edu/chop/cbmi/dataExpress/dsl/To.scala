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

import exceptions.UnsupportedStoreType
import edu.chop.cbmi.dataExpress.dsl.stores.{FileStore, SqlDb, Store}
import edu.chop.cbmi.dataExpress.dataWriters.DataWriter
import edu.chop.cbmi.dataExpress.dataModels.{SeqColumnNames, DataRow, DataType}
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

  def append() = {
    target match {
      case (fs: FileStore)=>{
        val writer = DataWriter(fs.fb, row.columnNames)
        writer.insert_row("",row)
      }
      case _ => throw UnsupportedStoreType(target, "insert_row")
    }
  }

  def append(table_name : String) = {
    target match {
      case (s: SqlDb) => {
        val schema = s.schema
        val catalog = s.catalog.getOrElse(null)
        val writer = DataWriter(s.backend, schema, catalog)
        writer.insert_row(table_name, row)
      }
      //case (s:ExcelStore) =>
      case _ => throw UnsupportedStoreType(target, "insert_row")

    }
  }
}

case class ToFromTable(target : Store, source_table : TransformableDataTable) extends To(target) {
  private val CREATE_MODE = 'create
  private val APPEND_MODE = 'append
  private val DEFAULT_BATCH_SIZE = 50
  //private val OVERWRITE_MODE = 'overwrite

  private def writeToTarget(table_name: String, mode:Symbol) : Boolean = {
    val col_names = source_table._final_column_names

    mode match{
      case CREATE_MODE => {
        val data_types = source_table._final_data_types match{
           case Some(l:List[DataType]) => l
           case _ => source_table.data_table.dataTypes.toList
         }
        target match{
          //TODO note the createTable method in Sql backend currently forces a drop table
          case s: SqlDb => s.backend.createTable(table_name, col_names, data_types, s.schema)
          case fs: FileStore => {
            fs.fb.delete()
            fs.fb.makeNewFile()
            if(fs.writeHeaderOnCreate){
              fs.fb.writeHeader(DataRow(col_names.map{cn => (cn,cn)}: _*))
            }
          }
          case _ => throw UnsupportedStoreType(target, "writeToTarget")
        }//end target match
         writeToTarget(table_name, APPEND_MODE)
       } //end CREATE_MODE
       case APPEND_MODE => {
         val writer = target match{
           case s:SqlDb => DataWriter(s.backend, s.schema, s.catalog.getOrElse(null))
           case fs: FileStore => DataWriter(fs.fb, col_names)
         }
         if(source_table._transformers.isEmpty){
           //just insert the table wholesale
           writer.insert_rows(table_name, source_table.data_table).operation_succeeded_?
         }else{
           val buffer = new ListBuffer[DataRow[Any]]()

           //fold left over all rows so we can accumulate an AND'ed boolean to know if all batch inserts succeeded
           val status = (true /: source_table.data_table){(b,next_row) =>
              //transform row
             (Some(next_row).asInstanceOf[Option[DataRow[Any]]] /: source_table._transformers){ (output, f) =>
               output match{
                 case Some(r:DataRow[Any]) => f(r)
                 case _ => None
               }
             }match{
               //add row to buffer if not filtered
               case Some(r) => buffer += r
               case _ => {} //TODO maybe log filtered rows?
             }
             if(buffer.length % DEFAULT_BATCH_SIZE == 0){
               //batch insert the buffer
               val success = writer.insert_rows(table_name, buffer.toList).operation_succeeded_?
               //TODO if !success log failure, rollback or some such actions
               buffer.clear
               b && success
             }else b
           }//end fold left
           //flush buffer if not empty
           if(buffer.length != 0){
             //batch insert the buffer
             status && (writer.insert_rows(table_name, buffer).operation_succeeded_?)
           }else status
         }
       } //end APPEND_MODE
     } //end mode match
  }

  def create(table_name : String) = writeToTarget(table_name, CREATE_MODE)

  def append(table_name : String) = writeToTarget(table_name, APPEND_MODE)

  def create() = {
    target match{
      case (s:FileStore) => writeToTarget("", CREATE_MODE)
      case _ => throw UnsupportedStoreType(target, "To.create(). Try create table_name?")
    }
  }

  def append() = {
    target match{
      case (s:FileStore) => writeToTarget("", APPEND_MODE)
      case _ => throw UnsupportedStoreType(target, "To.append(). Try append table_name?")
    }
  }
}