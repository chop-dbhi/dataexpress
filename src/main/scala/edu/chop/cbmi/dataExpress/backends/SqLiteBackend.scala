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
import edu.chop.cbmi.dataExpress.dataModels.DataRow
import java.util.Properties
import java.sql.PreparedStatement


class SqLiteBackend(override val connectionProperties : Properties, _sqlDialect : SqlDialect = null, _driverClassName : String = null)
  extends SqlBackend(connectionProperties, if(_sqlDialect==null)SqLiteDialect else _sqlDialect,
    if(_driverClassName==null)"org.sqlite.JDBC" else _driverClassName) {

  //SQLite does not support returning keys, so we will use an ordinary execute for this operation
  override def executeReturningKeys(sqlStatement:String, bindVars: Seq[Option[_]]): DataRow[_] = {
    super.execute(sqlStatement, bindVars)
    DataRow.empty
  }
  

  override val SUPPORTS_MULT_RS = false

  override protected def prepStatement(sqlStatement: PreparedStatement, bindVars: Seq[Option[_]]) = {
    if (bindVars.length > 0) {
      val vars = bindVars.zipWithIndex
      for (v <- vars) {
        //TODO: Too many edge cases in here, need to explicity set some more date/time stuff
        v._1 match {
          case Some(i: java.sql.Timestamp)    => sqlStatement.setString((v._2 + 1), i.toString())
          case Some(i: java.sql.Time)         => sqlStatement.setString((v._2 + 1), i.toString())
          case Some(i: java.sql.Date)		  => sqlStatement.setString((v._2 + 1), java.text.DateFormat.getDateInstance().format(i))
          //TODO: Test the java.util.Date for precision here to avoid trying to set to a higher precision
          case Some(i: java.util.Date)        => sqlStatement.setString((v._2 + 1), java.text.DateFormat.getDateTimeInstance().format(i))
          case None => sqlStatement.setNull(v._2 + 1, java.sql.Types.NULL)  //TODO: do something better here
          case _    => sqlStatement.setObject((v._2 + 1), v._1.get)
        }
      }
    }
  }
}
