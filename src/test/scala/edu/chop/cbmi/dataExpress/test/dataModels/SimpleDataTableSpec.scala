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
    it("should be an Iterator Seq of DataRow[Option[T]]") {
      Given("one or more rows, all of a HOMOGENEOUS type, say Int")
      val col_names = List("c1", "c2")
      val row_1 = List(1, 2)
      val row_2 = List(3, 4)
      val sdt1 = DataTable(col_names, row_1, row_2)

//No longer applies with the switch to Iterator
//      When("accessed by index, should return a DataRow[Option[Int]]")
//      val sdt1_r1 = sdt1(0)
//      assert(3 == ((sdt1_r1.head.get /: sdt1_r1.tail)(_ + _.get)))

      Then("the row elements should be accessible using the column names with dot notation")
      val sdt1_r1 = sdt1.next
      sdt1_r1.c1.get should equal(1)
      sdt1_r1.c2.get should equal(2)

      And("also with map notation")
      sdt1_r1("c1").get should equal(1)

      And("the row elements should be accessible using index notation")
      sdt1_r1(0).get should equal(1)

//      When("accessed as an iterable, should require a f(dr:DataRow[Option[Int]])=>_")
//      var i = 0
//      sdt1.foreach((dr: DataRow[Int]) => {
//        dr.foreach((d: Option[Int]) => {
//          i += 1
//          d.get should equal(i)
//        })
//      })

      Given("one ore more rows with HETEROGENEOUS types")
      val row_3 = List(1.0, testClass(2.5))
      val row_4 = List(3.0, testClass(4.5))
      val sdt2 = DataTable(col_names, row_3, row_4)

      When("accessed by index, should return a DataRow[Option[Any]]")
      val sdt2_r1 = sdt2(0)
      val s = (sdt2_r1.head.as[Double].get /: sdt2_r1.tail)(_ + _.as[testClass].get.d)
      s should equal(3.5)

//      When("accessed as an interable, should require a f(dr:DataRow[Option[_]])=>_")
//      var i = 0
//      sdt2.foreach((dr: DataRow[_]) => {
//        dr.foreach((o: Option[_]) => {
//          i += 1
//          val half = if (i % 2 == 0) 0.5 else 0
//          val dv = if (i % 2 != 0) o.get else o.as[testClass].get.d
//          dv should equal(i + half)
//        })
//      })
      When("rows are accessed with Iterator methods, it should advance through the rows")
      val itr_row1 = List(1.0, "foo", testClass(2.5))
      val itr_row2 = List(2.0, "bar", testClass(4.5))
      val itr_tbl = DataTable(List("col1", "col2", "col3"), itr_row1, itr_row2)
      itr_tbl.hasNext should be(true)
     
      val itr_dt_r1 = itr_tbl.next()
      itr_dt_r1(0).asu[Double] should equal (1.0)
      itr_dt_r1(1).asu[String] should equal ("foo")
      itr_dt_r1(2).asu[testClass] should equal (testClass(2.5))
      
      val itr_dt_r2 = itr_tbl.next()

      itr_dt_r2(0).asu[Double] should equal (2.0)
      itr_dt_r2(1).asu[String] should equal ("bar")
      itr_dt_r2(2).asu[testClass] should equal (testClass(4.5))
      itr_tbl.hasNext should be(false)
      
      
      When("accessed using dot notation for specific columns, it should return a Seq[Option[_]]")
      val y = sdt2.c1
      val x = sdt2.c2
      val XFirstVal = x.next
      val YFirstVal =  y.next
      
      YFirstVal.asu[Double] should equal(1.0)
      ((YFirstVal.asu[Double] /: y)(_ + _.asu[Double])) should equal(4.0)

 
      XFirstVal.asu[testClass].d should equal(2.5)
      (XFirstVal.asu[testClass].d /: x)(_ + _.asu[testClass].d) should equal(7.0)

      When("accessed using the col method, it should return a Seq[Option[_]]")
      val sdt2_c1 = sdt2.col("c1")
      sdt2_c1.next.get should equal(1.0)

      When("accessed using the col_as[G] method, it should return a Seq[Option[G]]")
      val sdt2_c2 = sdt2.col_as[testClass]("c2")
      val sdt2_c2_sum = (sdt2_c2.next.get.d /: sdt2_c2)(_ + _.get.d)
      sdt2_c2_sum should equal(7.0)

      When("accessed using the col_asu[G] method, it should return a Seq[G]")
      val sdt2_c2u = sdt2.col_asu[testClass]("c2")
      val sdt2_c2u_sum = (sdt2_c2u.next.d /: sdt2_c2u)(_ + _.d)
      sdt2_c2u_sum should equal(7.0)

      When("trying to access columns that don't exist should throw ColumnDoesNotExist for all access methods")
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