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
package edu.chop.cbmi.dataExpress.logging

import java.io.FileWriter
import java.io.BufferedWriter

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 2/2/12
 * Time: 10:58 AM
 * To change this template use File | Settings | File Templates.
 */

//Change class name to uppercase
case class  Log(fileWriter : BufferedWriter, logType: String) {
  var on:Boolean                    = true
  private val sqlInsertLogPrepend: String   = "INSERT INTO " + "<identifierQuote>" + "<schemaName>" + "<identifierQuote>"   +
                                              "."                                                                           +
                                              "<identifierQuote>" + "<tableName>" + "<identifierQuote>"                     +
                                              " VALUES ("

  private val sqlInsertLogAppend: String    = ")" + ";" +  "\n"


  def close()   = fileWriter.close()

  def write(output: String)   = {
    try {
      if (on)  fileWriter.write(output + "\n")
    }
    catch {
      case e:java.io.IOException  =>  throw new RuntimeException("IO Error writing to log file" + "\n")

    }
  }


  def bindVarsSqlInsertWrite(bindVars: Seq[Option[_]])   = {
    try {

      if (on)  {
        fileWriter.write(
          sqlInsertLogPrepend                                                                           +
          (for (i <- bindVars ) yield  {"'" + i.getOrElse(null) + "'"}).toString.drop(5).dropRight(1)   +
          sqlInsertLogAppend
        )
      }
    }
    catch {
      case e:java.io.IOException  =>  throw new RuntimeException("IO Error writing to log file" + "\n")

    }
  }

   def batchUpdateExceptionWrite(e: java.sql.BatchUpdateException)   = {
    var exceptionNumber: Int = 1
    try  if (on)  {
       fileWriter.write("Exception: " + exceptionNumber  + ":" + e.getMessage + "\n" + e.getCause + "\n" +
         e.getSQLState + "\n" + e.getErrorCode + "\n")
    }
    catch {
      case e:java.io.IOException  =>  throw new RuntimeException("IO Error writing to log file" + "\n")

    }
  }

}


object Log{

  def apply(simple_output_file: String, error_log: Boolean, sql_log: Boolean) : Log = {
    try {
        val simple_output_file_writer = new FileWriter(simple_output_file)
        val simple_output_file_buffered_writer = new BufferedWriter(simple_output_file_writer)
        if      (sql_log)   { new Log(simple_output_file_buffered_writer, "sql_log")    }
        else if (error_log) { new Log(simple_output_file_buffered_writer, "error_log")  }
        else                { new Log(simple_output_file_buffered_writer, "error_log")    }
    }
    catch {
        case e:java.io.IOException  => throw new RuntimeException(simple_output_file + " not found" + "\n")
        case e:RuntimeException     => throw new RuntimeException
    }
  }

   def apply(error_log: Boolean, sql_log: Boolean) : Log = {
    val simple_output_file        = System.getProperty("user.dir")
    val simple_output_file_writer = new FileWriter(simple_output_file       +
          (if (sql_log) "/sql.log" else if (error_log) "/error.log" else "/error.log") )
    val simple_output_file_buffered_writer = new BufferedWriter(simple_output_file_writer)
    try {
        if      (sql_log)   { new Log(simple_output_file_buffered_writer, "sql_log")    }
        else if (error_log) { new Log(simple_output_file_buffered_writer, "error_log")  }
        else                { new Log(simple_output_file_buffered_writer, "error_log")    }
    }
    catch {
        case e:java.io.IOException  => throw new RuntimeException(simple_output_file + " not found" + "\n")
        case e:RuntimeException     => throw new RuntimeException
    }
  }


}

