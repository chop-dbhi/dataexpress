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
import edu.chop.cbmi.dataExpress.dataModels.sql._
import java.sql.ResultSetMetaData
import edu.chop.cbmi.dataExpress.dataModels.DataType
import collection.Seq

/**
 * Created by IntelliJ IDEA.
 * User: italiam
 * Date: 8/4/11
 * Time: 3:09 PM
 * To change this template use File | Settings | File Templates.
 */

trait SqlDialect {

  def quoteIdentifier(id: String) : String

  def createTable(name: String, columns: List[(String,DataType)], schemaName:Option[String]) : String

  def dropTable(name: String, cascade:Boolean=false, schemaName:Option[String] =  None ) : String

  def truncate(table: String, schemaName:Option[String] =  None) : String

  def commit()  : String

  def rollback() : String


  def endTransaction() : String

  def startTransaction() : String

  def insertRecord(tableName: String, columnNames:List[String], schemaName:Option[String] = None) : String

  def updateRecords(tableName: String, columnNames:List[String],
                    filter:List[(String, Any)], schemaName:Option[String]  = None) : String

  def toSqlString(dataType: DataType) : String

  def mapDataTypes(column_names : Seq[String], meta : ResultSetMetaData) =  {
    column_names map((name:String)=>column_names.indexOf(name)) map((j:Int) => {
      val i = j + 1
      meta.getColumnType(i) match {
        //TODO: Add BIGINT support!
        case java.sql.Types.INTEGER                           =>  IntegerDataType()
        case java.sql.Types.SMALLINT                          =>  SmallIntegerDataType()
        case java.sql.Types.TINYINT                           =>  TinyIntegerDataType()
        case java.sql.Types.FLOAT                             =>  {
          val precision   = meta.getPrecision(i)
          FloatDataType(precision)
        }
        case java.sql.Types.REAL |  java.sql.Types.DOUBLE     =>  {
          val precision   = meta.getPrecision(i)
          FloatDataType(precision)
        }
        case java.sql.Types.NUMERIC | java.sql.Types.DECIMAL  =>  {
          val precision = meta.getPrecision(i)
          val scale = meta.getScale(i)
          //fix for Oracle FLOATS
          if (scale == -127) FloatDataType(precision) else DecimalDataType(precision, scale)
        }
        case java.sql.Types.CHAR                              =>  CharacterDataType(meta.getColumnDisplaySize(i), true)
        case java.sql.Types.VARCHAR                           =>  CharacterDataType(meta.getColumnDisplaySize(i), false)
        //TODO: use the meta.getColumnTypeName to get the SQL data type and look for the time zone using a regex
        case java.sql.Types.TIMESTAMP                         =>  {
          val tzSupport = meta.getColumnTypeName(i).toUpperCase.contains("WITH TIME ZONE")
          DateTimeDataType(tzSupport)
        }
        case -101                                             =>  DateTimeDataType(true)     //-101 = jdbc: TIME STAMP WITH TIME ZONE
        case java.sql.Types.DATE                              =>  DateDataType()
        case java.sql.Types.TIME                              =>  {
          val tzSupport = meta.getColumnTypeName(i).toUpperCase.contains("WITH TIME ZONE")
          TimeDataType(tzSupport)
        }
        case java.sql.Types.LONGVARCHAR | java.sql.Types.CLOB =>  TextDataType()
        case java.sql.Types.LONGVARBINARY | java.sql.Types.BLOB | java.sql.Types.BINARY => BigBinaryDataType()
        case java.sql.Types.BOOLEAN                           =>  BooleanDataType()
        // JDBC Spec suggests that portable code should represent the BIT type as
        // a smallInt: http://download.oracle.com/javase/6/docs/technotes/guides/jdbc/getstart/mapping.html#999005
        //Postgres maps Boolean to Bit and so data such as 't' for true are failing on the attempted storage as integer
        //values with this one:
        //case java.sql.Types.BIT                               => SmallIntegerDataType()
        //Using this instead so that we can handle the variations on a BIT datatype on a DBMS to DBMS basis
        case java.sql.Types.BIT                               => BitDataType()
        case _                                                =>  {
          throw new RuntimeException("Can't map JDBC type to a known DataExpress type " +
            meta.getColumnType(i) + " for column " + meta.getColumnName(i))
        }
      }
    })
  }

}