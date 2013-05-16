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

import edu.chop.cbmi.dataExpress.dsl.exceptions.{UnsupportedStoreType}
import edu.chop.cbmi.dataExpress.dsl.stores.{FileStore, SqlDb, Store}
import edu.chop.cbmi.dataExpress.dataModels.{DataTable}

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 12/29/11
 * Time: 4:11 PM
 * To change this template use File | Settings | File Templates.
 */

case class From private[dsl](private val store : Store) {

  def get_table(table_name: String): FixedDimensionTransformableTable = store match {
    case (s: SqlDb) => {
      val query = s.schema match {
        case None => """SELECT * FROM %s""".format(s.backend.sqlDialect.quoteIdentifier(table_name))
        case Some(sch) => """SELECT * FROM %s.%s""".format(
          s.backend.sqlDialect.quoteIdentifier(sch),s.backend.sqlDialect.quoteIdentifier(table_name))
      }
      new FixedDimensionTransformableTable(DataTable(s.backend, query))
    }
    case _ => throw UnsupportedStoreType(store, "get_table")
  }

  def query(q: String, bindvars : Seq[Option[Any]] = Seq.empty[Option[Any]]): FixedDimensionTransformableTable = store match {
    case (s:SqlDb) => new FixedDimensionTransformableTable(DataTable(s.backend, q, bindvars))
    case _ => throw UnsupportedStoreType(store, "query")
  }

  def get_values() = store match{
    case (s: FileStore) => new FixedDimensionTransformableTable(DataTable(s.fb, s.cng))
    case _ => throw UnsupportedStoreType(store, "get_values")
  }

}
