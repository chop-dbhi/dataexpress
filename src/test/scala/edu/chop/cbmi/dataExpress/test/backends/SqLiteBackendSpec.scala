package edu.chop.cbmi.dataExpress.test.backends


import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{GivenWhenThen, Spec, FunSuite}
import java.io.InputStream
import java.util.{Properties, ResourceBundle}
import edu.chop.cbmi.dataExpress.test.util._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import edu.chop.cbmi.dataExpress.backends.SqLiteBackend
import org.scalatest.FunSpec

@RunWith(classOf[JUnitRunner])
class SqLiteBackendSpec extends FunSpec with ShouldMatchers with GivenWhenThen  {

  def fixture =
    new {
	  	val inputStream = this.getClass().getResourceAsStream("sqlite_test.properties")
        val props = new Properties()
        props.load(inputStream)
        inputStream.close()
    }
  describe("Sqlite backend") {
    val f = fixture



    it("should have a null connection to start") {
        val backend = new SqLiteBackend(f.props)
        backend.connection should be (null)
        backend.close()
    }

    it("should connect using a Properties object") {
        val backend             = new SqLiteBackend(f.props)
        backend.connect()
        backend.connection should not be (null)
        backend.close()
    }




    it("should throw an exception if one of the properties isn't provided") {
        val backend             = new SqLiteBackend(f.props)
        val badProps            = new Properties()
        badProps.putAll(f.props)
        badProps.remove("user")
        evaluating {backend.connect()} should produce [RuntimeException]
        backend.close()
    }


    it("should have a closed connection after it closes the connection") {
        val newFixture          = fixture
        val backend             = new SqLiteBackend(newFixture.props)
        backend.connect()
        backend.connection should not be (null)
        backend.close()
        backend.connection.isClosed() should be (true)

    }






  }

}

