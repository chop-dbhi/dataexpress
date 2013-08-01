package edu.chop.cbmi.dataExpress.backends.test

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{GivenWhenThen, FunSpec, Tag}
import edu.chop.cbmi.dataExpress.test.util._
import edu.chop.cbmi.dataExpress.backends.OracleBackend
import scala.language.reflectiveCalls
import java.util.Properties

class OracleBackendSpec extends FunSpec with ShouldMatchers with GivenWhenThen  {


  def fixture =
    new {
      //Need to make sure we don't try to load properties inside travis_ci
      val travis = System.getenv("TRAVIS")
      val props = travis match {
        case "true"=> new Properties()
        case _ => TestProps.getDbProps("oracle")
      }
    }

  describe("Oracle backend") {
    val f = fixture



    it("should have a null connection to start", OracleTest) {
        val backend = new OracleBackend(f.props)
        backend.connection should be (null)
        backend.close()
    }

    it("should connect using a Properties object", OracleTest) {
        val backend             = new OracleBackend(f.props)
        backend.connect()
        backend.connection should not be (null)
        backend.close()
    }

    it("should throw an exception if one of the properties isn't provided", OracleTest) {
    	val badProps = f.props
        badProps.remove("jdbcUri")
    	val backend = new OracleBackend(badProps) 
        evaluating {backend.connect()} should produce [RuntimeException]
        backend.close()
    }

    it("should have a closed connection after it closes the connection", OracleTest) {
        val newFixture          = fixture
        val backend             = new OracleBackend(newFixture.props)
        backend.connect()
        backend.connection should not be (null)
        backend.close()
        backend.connection.isClosed() should be (true)

    }
  }

}

