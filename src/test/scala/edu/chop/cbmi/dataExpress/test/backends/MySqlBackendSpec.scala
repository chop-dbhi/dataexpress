package edu.chop.cbmi.dataExpress.test.backends

/**
 * Created by IntelliJ IDEA.
 * User: italiam
 * Date: 11/22/11
 * Time: 1:22 PM
 * To change this template use File | Settings | File Templates.
 */
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{GivenWhenThen, Spec}
import java.util.Properties
import edu.chop.cbmi.dataExpress.backends.MySqlBackend
import edu.chop.cbmi.dataExpress.test.util._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
@RunWith(classOf[JUnitRunner])
class MySqlBackendSpec extends Spec with ShouldMatchers with GivenWhenThen  {

  def fixture =
    new {
	  	val inputStream = this.getClass().getResourceAsStream("mysql_test.properties")
        val props = new Properties()
        props.load(inputStream)
        inputStream.close()
    }
  describe("MySql backend") {
    val f = fixture



    it("should have a null connection to start") {
        val backend = new MySqlBackend(f.props)
        backend.connection should be (null)
        backend.close()
    }

    it("should connect using a Properties object") {
        val backend             = new MySqlBackend(f.props)
        backend.connect()
        backend.connection should not be (null)
        backend.close()
    }




    it("should throw an exception if one of the properties isn't provided") {
        val backend             = new MySqlBackend(f.props)
        val badProps            = new Properties()
        badProps.putAll(f.props)
        badProps.remove("user")
        evaluating {backend.connect()} should produce [RuntimeException]
        backend.close()
    }


    it("should have a closed connection after it closes the connection") {
        val newFixture          = fixture
        val backend             = new MySqlBackend(newFixture.props)
        backend.connect()
        backend.connection should not be (null)
        backend.close()
        backend.connection.isClosed() should be (true)

    }






  }

}

