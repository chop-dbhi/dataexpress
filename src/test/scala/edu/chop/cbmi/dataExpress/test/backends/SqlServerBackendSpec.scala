package edu.chop.cbmi.dataExpress.test.backends

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import edu.chop.cbmi.dataExpress.test.util._
import edu.chop.cbmi.dataExpress.backends.SqlServerBackend
import scala.language.reflectiveCalls


class SqlServerBackendSpec extends FunSpec with ShouldMatchers  {

  def fixture =
    new {
      val props = TestProps.getDbProps("sqlserver")
    }
  describe("SqlServer backend") {
    lazy val f = fixture



    it("should have a null connection to start", SqlServerTest) {
      val backend = new SqlServerBackend(f.props)
      backend.connection should be (null)
      backend.close()
    }

    it("should connect using a Properties object", SqlServerTest) {
      val backend  = new SqlServerBackend(f.props)
      backend.connect()
      backend.connection should not be (null)
      backend.close()
    }

    it("should throw an exception if one of the properties isn't provided", SqlServerTest) {
      val badProps = f.props
      badProps.remove("jdbcUri")
      val backend = new SqlServerBackend(badProps)
      evaluating {backend.connect()} should produce [RuntimeException]
      backend.close()
    }


    it("should have a closed connection after it closes the connection", SqlServerTest) {
      val newFixture          = fixture
      val backend             = new SqlServerBackend(newFixture.props)
      backend.connect()
      backend.connection should not be (null)
      backend.close()
      backend.connection.isClosed() should be (true)

    }






  }

}
