package edu.chop.cbmi.dataExpress.test.backends

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FunSpec
import edu.chop.cbmi.dataExpress.test.util._
import edu.chop.cbmi.dataExpress.backends.PostgresBackend
import scala.language.reflectiveCalls

class PostgresBackendSpec extends FunSpec with ShouldMatchers  {

  def fixture =
    new {
	  val props = TestProps.getDbProps("postgres")
    }
  describe("Postgres backend") {
    val f = fixture


    it("should have a null connection to start") {
        val backend = new PostgresBackend(f.props)
        backend.connection should be (null)
        backend.close()
    }

    it("should connect using a Properties object") {
        val backend             = new PostgresBackend(f.props)
        backend.connect()
        backend.connection should not be (null)
        backend.close()
    }

    it("should throw an exception if one of the properties isn't provided") {
        val badProps = f.props
        badProps.remove("jdbcUri")
    	val backend = new PostgresBackend(badProps) 
        evaluating {backend.connect()} should produce [RuntimeException]
        backend.close()
    }

    it("should have a closed connection after it closes the connection") {
        val newFixture          = fixture
        val backend             = new PostgresBackend(newFixture.props)
        backend.connect()
        backend.connection should not be (null)
        backend.close()
        backend.connection.isClosed() should be (true)

    }
  }

}

