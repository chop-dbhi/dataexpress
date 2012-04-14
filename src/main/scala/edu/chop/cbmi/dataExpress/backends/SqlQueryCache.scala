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
package edu.chop.cbmi.dataExpress.backends

import java.sql.Statement

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 1/27/12
 * Time: 12:23 PM
 * To change this template use File | Settings | File Templates.
 */

object SqlQueryCache {
  def apply(size: Int, connection:java.sql.Connection) = {
    new SqlQueryCache(size,connection)
  }
}

class SqlQueryCache(size: Int, connection:java.sql.Connection) {
  val statementMap = new scala.collection.mutable.LinkedHashMap[String,java.sql.PreparedStatement]()
  val statementsWithKeysMap = new scala.collection.mutable.LinkedHashMap[String,java.sql.PreparedStatement]()

  def prepReturningKeys(sql:String) = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
  def prepSimpleStatement(sql:String) = connection.prepareStatement(sql)

  private def prepStatement(sql: String, returnKeys: Boolean) = {
    val (prepareMethod, cache) = returnKeys match {
      case true => (prepReturningKeys(sql), statementsWithKeysMap)
      case false => (prepSimpleStatement(sql), statementMap)
    }

    val preparedStatement = cache.getOrElseUpdate(sql, prepareMethod)

    if (cache.size > size) {
      cache.head._2.close()
      cache.remove(cache.head._1)
    }

    preparedStatement
  }

  def getStatement(sql: String): java.sql.PreparedStatement = {
     prepStatement(sql, false)
  }

  def getStatementReturningKeys(sql: String): java.sql.PreparedStatement = {
    prepStatement(sql, true)
  }
}