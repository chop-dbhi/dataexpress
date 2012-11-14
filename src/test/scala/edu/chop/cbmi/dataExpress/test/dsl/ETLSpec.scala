package edu.chop.cbmi.dataExpress.test.dsl

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfter, GivenWhenThen, Spec}
import edu.chop.cbmi.dataExpress.test.util.TestProps
import edu.chop.cbmi.dataExpress.dsl.ETL
import edu.chop.cbmi.dataExpress.dsl.ETL._
import edu.chop.cbmi.dataExpress.dsl.stores.{Store, SqlDb}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSpec

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 12/28/11
 * Time: 8:40 AM
 * To change this template use File | Settings | File Templates.
 */

class ETLSpec extends FunSpec with GivenWhenThen with ShouldMatchers with BeforeAndAfter{

  def fixture(db_type : String) = {
    new {
      val prop_file = db_type match {
        case "POSTGRES" => TestProps.getDbPropFilePath("postgres")
        case "MYSQL" =>  TestProps.getDbPropFilePath("mysql")
        case _ => throw new Exception("Invalid db_type selection")
      }
      val sqlBackend = db_type match {
        case "POSTGRES" => TestProps.postGresTestDB
        case "MYSQL" => TestProps.mySqlTestDB
      }
    }
  }

  //val f = fixture("POSTGRES")
  val f = fixture("MYSQL")

  describe("The ETL object") {
    it("should execute and valid code in an execute block if on is true") {
      var x = 0
      ETL.execute(true) {
        x = 1
        val y = 2
        x + y
      } should equal(Some(3))
      x should equal(1)
    }

    it("should not execute the code if on is false"){
      var x = 1
      ETL.execute(false){
        x = 2
      } should equal(None)
      x should equal(1)
    }

    it("should open a store when register is called"){
      given("a valid store type")
      var s : Store = null
      ETL.execute(true){
        s = registerStore(SqlDb(f.prop_file))
        s.is_closed_? should equal(false)
      }
      and("close the store when the execute block is done if cleanup is true which is the default")
      s.is_closed_? should equal(true)

      and("leave the store open when the execute block is done if cleanup is false")
      ETL.execute(true, false){
        s = registerStore(SqlDb(f.prop_file))
      }
      s.is_closed_? should equal(false)
      s.close
      s.is_closed_? should equal(true)

      and("have zero registered stores after cleanup")
      ETL.cleanup()
      ETL.registered_store_count should equal(0)

      and("only register a given store once")
      ETL.registerStore(SqlDb(f.prop_file))
      ETL.registered_store_count should equal(1)
      ETL.registerStore(SqlDb(f.prop_file))
      ETL.registered_store_count should equal(1)
      //TODO this should be tested against registering a second valid store
      ETL.cleanup()
    }

  }


}