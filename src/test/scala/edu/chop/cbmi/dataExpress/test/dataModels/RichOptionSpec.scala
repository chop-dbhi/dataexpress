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

import org.scalatest.{GivenWhenThen, Spec}
import org.scalatest.matchers.ShouldMatchers
import edu.chop.cbmi.dataExpress.dataModels.RichOption._

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 11/4/11
 * Time: 3:24 PM
 * To change this template use File | Settings | File Templates.
 */

class RichOptionSpec extends Spec with GivenWhenThen with ShouldMatchers {

  private case class testClass(d: Double)

  describe("A RichOption Object") {
    it("should implicitly convert Option[T] objects to RichOption[T] objects") {
      given("a None Object it should return a RichOption(None)")
      val x = None
      x.as[testClass] match {
        case None => assert(true)
        case _ => assert(false)
      }

      given("a list of elements Some() of mixed type, the result is List[Some(Any)]")
      val y: List[Some[Any]] = List(Some(1), Some(testClass(1)))

      when("a variable, say x, is assigned to a specific element of that list, with result type Some[Any]")
      val y1: Some[Any] = y(1)

      and("x.as[T] is called, the result should be Some[T]")
      y1 match {
        case Some(testClass(1)) => assert(true)
        case _ => assert(false)
      }

      type tc = testClass
      (y1.asu[tc].d) should equal(1)

      (y1.as[tc].get.d) should equal(1)
    }

  }

}