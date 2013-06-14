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

import java.io.FileNotFoundException
import edu.chop.cbmi.dataExpress.dataModels.{DataRow, DataTable}
import statements.{InsertSelect, GetSelect, CopySelect}
import stores.{RegisterAsPre, Store}
import scala.language.implicitConversions

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 12/28/11
 * Time: 8:38 AM
 * To change this template use File | Settings | File Templates.
 */

object ETL {

  private val registered_stores = scala.collection.mutable.Map.empty[Any,Store]
 // private val user_store_names = scala.collection.mutable.Map.empty[String, Any]

  def execute[Q](on: Boolean, do_cleanup: Boolean = true)(code: => Q): Option[Q] = {
    if (on) {
      try {
        val r = Some(code)
        r
      }
        catch {
          case ex : Exception => {
            "execute exception %s".format(println(ex.getMessage))
            None
          }
          case _:Throwable => None
        }
      finally {
        if(do_cleanup)cleanup()
      }
    } else None
  }

  def commit_on_success (stores_to_commit : Store*)(code: => Any) : Boolean = {
    try{
      code
      stores_to_commit foreach {_.save}
      true
    }catch{
      //TODO should there be rollback or option to rollback here?
      case e:Exception => {
        throw new java.lang.RuntimeException(e.getMessage())
        false
      }
    }
  }

  def cleanup() = {
    try {
      registered_stores.values.foreach(_.close)
    }
    catch {
      case (fnf: FileNotFoundException) => println("Warning: Unable to close: " + fnf.getMessage)
    }
    registered_stores.clear
  }

  def registerStore(f: Store): Store = {
    val key = f.unique_id
    val store = registered_stores.get(key) match {
      case Some(s: Store) => {
    	  if (!s.is_closed_?) {
    	    println("Warning: You are creating a data store %s but %s is already a store with an open connection that will now be closed.".format(f.unique_id, f.unique_id))
    	    s.close
    	  }
    	  registered_stores.put(key, f)
    	  f
      }
      case None => {
        registered_stores += key -> f
        f
      }
    }
    store.open
    store
  }

  def registered_store_count : Int = (0 /: registered_stores.values)((v:Int, s:Store)=>v+1)

  //Access methods
  def copy = new CopySelect
  def get = new GetSelect
  def register = new RegisterAsPre
  def insert = new InsertSelect

  //Convenience methods
  def new_row(column_name_value_pair : (String,_)*) = DataRow(column_name_value_pair: _*)

  //Implicits
  //implicit def dataTable2FixedDimDataTable(dt : DataTable[_]) = new FixedDimensionTransformableTable(dt)

  implicit def string2Store(name: String) = registered_stores.get(name) match {
    case Some(s:Store) =>s
    case _ => throw new Exception("Store named " + name + " not registered.")
  }

}