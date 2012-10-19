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
package edu.chop.cbmi.dataExpress.dataModels.sql

import java.sql.ResultSet

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 12/9/11
 * Time: 8:40 AM
 * To change this template use File | Settings | File Templates.
 */

abstract case class SqlIterator[+T] private[sql](private val sql_query_package: SqlQueryPackage) extends Iterator[T] {
  protected var cursor_advanced = false
  protected var more_rows = false

  lazy protected val result_set: ResultSet = sql_query_package.resultSet
  lazy protected val meta = sql_query_package.meta

  protected def generate_next() : T

  override def hasNext() = {
    if (!cursor_advanced) {
      cursor_advanced = true
      more_rows = result_set.next()
    }
    more_rows
  }

  override def next(): T = {
    if (cursor_advanced) cursor_advanced = false
    else more_rows = result_set.next()

    //Some databases will complain if a result set is not properly closed, need to generate the next set of values
    //then close the result set.
    val next = generate_next()
    if(!hasNext) {
      result_set.close()
    }
    next
  }


  def next_item_in_column(i: Int) = meta.getColumnType(i) match {
    //Postgres Boolean values such as 't' were having issues
    //because they are mapped to java.sql.Types.BIT
    //TODO: this code needs to be moved out of here
    case java.sql.Types.BIT => result_set.getBoolean(i)
    case _ => result_set.getObject(i)
  }
}