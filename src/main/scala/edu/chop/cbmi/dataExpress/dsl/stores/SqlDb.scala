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
package edu.chop.cbmi.dataExpress.dsl.stores

import edu.chop.cbmi.dataExpress.backends.{SqlDialect, SqlBackendFactory, SqlBackend}

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 12/28/11
 * Time: 8:48 AM
 * To change this template use File | Settings | File Templates.
 */

class SqlDb(val backend : SqlBackend, val schema : Option[String], val catalog : Option[String]) extends Store{

  private var _unique_id : Any = backend.get_jdbcUri

  override def is_closed_? : Boolean = backend.connection == null || backend.connection.isClosed

  override def open : Boolean = {
    if(is_closed_?)backend.connect()
    !is_closed_?
  }

  override def close : Boolean = {
    if(!is_closed_?)backend.close()
    is_closed_?
  }

  override def save : Boolean = {
    if(!is_closed_?)backend.commit()
    else false
  }

  override def set_unique_id(id : Any) = {
    _unique_id = id
  }

  override def unique_id = _unique_id

}

object SqlDb {

  def apply(backend : SqlBackend, schema : Option[String], catalog : Option[String]) =
    new SqlDb(backend, schema, catalog)

  def apply(prop_file_path: String, schema: Option[String] = None, catalog: Option[String] = None,
            sqlDialect : SqlDialect = null, driverClassName : String = null) : SqlDb = {
    apply(SqlBackendFactory(prop_file_path, sqlDialect, driverClassName), schema, catalog)
  }

  def apply(backend : SqlBackend) : SqlDb = apply(backend, None, None)

}