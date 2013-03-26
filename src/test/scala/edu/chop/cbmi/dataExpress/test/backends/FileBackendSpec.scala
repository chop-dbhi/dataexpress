package edu.chop.cbmi.dataExpress.test.backends

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.GivenWhenThen

import edu.chop.cbmi.dataExpress.backends.FileBackend

class FileBackendSpec extends FunSpec with GivenWhenThen with ShouldMatchers {

  describe("Reading CSV") {
    it("should open a local file") {
      val backend = new FileBackend("src/test/resources/presidents.csv")
      backend.openForRead()
      backend.close()
    }
  }
}
