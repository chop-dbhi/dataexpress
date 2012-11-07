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

import org.scalatest.{GivenWhenThen, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import edu.chop.cbmi.dataExpress.dataModels.RichOption._
import edu.chop.cbmi.dataExpress.dataModels._

import edu.chop.cbmi.dataExpress.exceptions.ColumnDoesNotExist

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 11/7/11
 * Time: 1:31 PM
 * To change this template use File | Settings | File Templates.
 */

class SimpleDataTableSpec extends FunSpec with GivenWhenThen with ShouldMatchers {

  private case class testClass(d: Double)

  describe("A SimpleDataTable") {
    it("should be an Iteralbe Seq of DataRow[Option[T]]") {
      given("one or more rows, all of a HOMOGENEOUS type, say Int")
      val col_names = List("c1", "c2")
      val row_1 = List(1, 2)
      val row_2 = List(3, 4)
      val sdt1 = DataTable(col_names, row_1, row_2)


      when("accessed by index, should return a DataRow[Option[Int]]")
      val sdt1_r1 = sdt1(0)
      assert(3 == ((sdt1_r1.head.get /: sdt1_r1.tail)(_ + _.get)))

      and("the row elements should be accessible using the column names with dot notation")
      sdt1_r1.c1.get should equal(1)
      sdt1_r1.c2.get should equal(2)

      and("also with map notation")
      sdt1_r1("c1").get should equal(1)

      and("the row elements should be accessible using index notation")
      sdt1_r1(0).get should equal(1)

      when("accessed as an iterable, should require a f(dr:DataRow[Option[Int]])=>_")
      var i = 0
      sdt1.foreach((dr: DataRow[Int]) => {
        dr.foreach((d: Option[Int]) => {
          i += 1
          d.get should equal(i)
        })
      })

      given("one ore more rows with HETEROGENEOUS types")
      val row_3 = List(1.0, testClass(2.5))
      val row_4 = List(3.0, testClass(4.5))
      val sdt2 = DataTable(col_names, row_3, row_4)

      when("accessed by index, should return a DataRow[Option[Any]]")
      val sdt2_r1 = sdt2(0)
      val s = (sdt2_r1.head.as[Double].get /: sdt2_r1.tail)(_ + _.as[testClass].get.d)
      s should equal(3.5)

      when("accessed as an interable, should require a f(dr:DataRow[Option[_]])=>_")
      i = 0
      sdt2.foreach((dr: DataRow[_]) => {
        dr.foreach((o: Option[_]) => {
          i += 1
          val half = if (i % 2 == 0) 0.5 else 0
          val dv = if (i % 2 != 0) o.get else o.as[testClass].get.d
          dv should equal(i + half)
        })
      })

      when("accessed using dot notation for specific columns, it should return a Seq[Option[_]]")
      val y = sdt2.c1
      y(0).get should equal(1.0)
      ((y.head.asu[Double] /: y.tail)(_ + _.asu[Double])) should equal(4.0)
      val x = sdt2.c2
      x(0).asu[testClass].d should equal(2.5)
      (x.head.asu[testClass].d /: x.tail)(_ + _.asu[testClass].d) should equal(7.0)

      when("accessed using the col method, it should return a Seq[Option[_]]")
      val sdt2_c1 = sdt2.col("c1")
      sdt2_c1(0).get should equal(1.0)

      when("accessed using the col_as[G] method, it should return a Seq[Option[G]]")
      val sdt2_c2 = sdt2.col_as[testClass]("c2")
      val sdt2_c2_sum = (sdt2_c2.head.get.d /: sdt2_c2.tail)(_ + _.get.d)
      sdt2_c2_sum should equal(7.0)

      when("accessed using the col_asu[G] method, it should return a Seq[G]")
      val sdt2_c2u = sdt2.col_asu[testClass]("c2")
      val sdt2_c2u_sum = (sdt2_c2u.head.d /: sdt2_c2u.tail)(_ + _.d)
      sdt2_c2u_sum should equal(7.0)

      when("trying to access columns that don't exist should throw ColumnDoesNotExist for all access methods")
      intercept[ColumnDoesNotExist] {
        sdt2.bad_name
      }
      intercept[ColumnDoesNotExist] {
        sdt2.col("bad_name")
      }
      intercept[ColumnDoesNotExist] {
        sdt2.col_as[Any]("bad_name")
      }
      intercept[ColumnDoesNotExist] {
        sdt2.col_asu[Any]("bad_name")
      }

    }
  }

}