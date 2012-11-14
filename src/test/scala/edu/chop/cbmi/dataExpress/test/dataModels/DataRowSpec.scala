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
package edu.chop.cbmi.dataExpress.test.dataModels

import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import edu.chop.cbmi.dataExpress.dataModels.DataRow
import edu.chop.cbmi.dataExpress.exceptions.ColumnDoesNotExist
import collection.mutable.ListBuffer

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 12/1/11
 * Time: 2:05 PM
 * To change this template use File | Settings | File Templates.
 */
class DataRowSpec extends FunSpec with GivenWhenThen with ShouldMatchers {

  describe("A DataRow object") {
    it("should function as a Seq with dot notation and map like access to elements") {
      given("a list of column names and a list of data items")
      val names = List("a", "b")
      val values = List(1, 2)
      val row = DataRow(names)(values map (Some(_)))

      when("treated as a normal seq, allow iterations")
      val sum = (row.head.get /: row.tail)(_ + _.get)
      sum should equal(3)

      when("accessed using dot notation with column names, enable dynamic method calls")
      (row.a.get + row.b.get) should equal(3)

      when("accessed using map notation with column names, enable the apply(string) call")
      (row("a").get + row("b").get) should equal(3)

      when("accessed using index notation, enable apply(int) call")
      (row(0).get + row(1).get) should equal(3)

      and("should throw a columnDoesNotExist exception if accessed with an invalid column name")
      intercept[ColumnDoesNotExist] {
        row.c
      }
      intercept[ColumnDoesNotExist] {
        row("c")
      }

      given("variable argument length of (String,Any) should return a DataRow[Any]")
      val m = ListBuffer("fn" -> "Jane", "ln" -> "Doe", "age" -> 10)
      val row2 = DataRow(m: _*)
      row2.fn should equal(Some("Jane"))
      row2.fn.get should equal("Jane")
      and("the DataRow should be immutable even though the map is mutable")
      m += "gender" -> "female"
      m(0) = "fn" -> "Jen"
      row2.fn.get should equal("Jane")
      intercept[ColumnDoesNotExist] {
        row2.gender
      }
    }
  }

}