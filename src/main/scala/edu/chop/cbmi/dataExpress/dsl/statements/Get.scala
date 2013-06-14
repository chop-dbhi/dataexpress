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
package edu.chop.cbmi.dataExpress.dsl.statements

import edu.chop.cbmi.dataExpress.dsl.stores.{FileStore, SqlDb, Store}
import edu.chop.cbmi.dataExpress.dsl.exceptions.UnsupportedStoreType
import edu.chop.cbmi.dataExpress.dataModels.{DataRow, DataTable}
import edu.chop.cbmi.dataExpress.dsl.From

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 1/13/12
 * Time: 12:28 PM
 * To change this template use File | Settings | File Templates.
 */

abstract class GetFrom{
  def from(source : Store) : DataTable[_]
}

class GetFromQuery(query : String) extends GetFrom{

  private var _bind_vars : Seq[Option[Any]] = Seq.empty[Option[Any]]

  def from(source:Store) = source match{
    case s:SqlDb => DataTable(s.backend, query, _bind_vars)
    case _ => throw UnsupportedStoreType(source, "GetFromQuery.from")
  }

  def using_bind_vars(bind_var : Any*) = {
    _bind_vars = bind_var.toSeq map {Some(_)}
    this
  }
}

class GetFromTable(table_name : String) extends GetFrom{
  def from(source : Store) = From(source).get_table(table_name)
}


class GetSelect {
  def query(q : String) : GetFromQuery = new GetFromQuery(q)
  def table(table_name : String) : GetFromTable = new GetFromTable(table_name)
  def from(source : Store) = From(source).get_values.data_table
}