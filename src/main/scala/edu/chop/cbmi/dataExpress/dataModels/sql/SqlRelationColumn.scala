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


/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 12/8/11
 * Time: 8:55 AM
 * To change this template use File | Settings | File Templates.
 */

case class SqlRelationColumn[G] private[sql](private val sql_query_package : SqlQueryPackage, f:(Any)=>G)
  extends Iterable[G]{

  override def iterator = {
    val query_package = SqlQueryPackage(sql_query_package.dataStore, sql_query_package.query, sql_query_package.bindVars)
    SqlRelationColumnIterator(query_package)
  }

  case class SqlRelationColumnIterator private[SqlRelationColumn](private val sql_query_package : SqlQueryPackage)
    extends SqlIterator[G](sql_query_package){

    override def generate_next() : G = {
      val item = next_item_in_column(1)
      f(item)
    }
  }
}


