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

import java.util.Properties
/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 8/19/11
 * Time: 9:17 AM
 * To change this template use File | Settings | File Templates.
 */


class MySqlBackend(override val connectionProperties : Properties, _sqlDialect : SqlDialect = null,
                           _driverClassName : String = null)
  extends SqlBackend(connectionProperties, if(_sqlDialect==null)MySqlDialect else _sqlDialect,
    if(_driverClassName==null)"com.mysql.jdbc.Driver" else _driverClassName) {

  /*this is over ridden because MySQL does not support multiple result sets. Thus it is necessary
    to manage result sets to ensure that any open result sets are closed before performing a query.
    The default SqlBackend implementation will manage these when this flag is set to false
  */
  override val SUPPORTS_MULT_RS = false

  //In order to allow streaming results, mySQL REQUIRES the fetch size to be -2^31 or Integer.MIN_VALUE, all other values are ignored
  override def executeQuery(sqlStatement: String, bindvars: Seq[Option[_]] = Seq.empty[Option[_]], fetchSize:Int=20): java.sql.ResultSet = {
    super.executeQuery(sqlStatement, bindvars, Integer.MIN_VALUE)
  }
}
