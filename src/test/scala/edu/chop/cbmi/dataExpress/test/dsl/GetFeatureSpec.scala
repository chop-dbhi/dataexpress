package edu.chop.cbmi.dataExpress.test.dsl

import edu.chop.cbmi.dataExpress.dsl.ETL
import edu.chop.cbmi.dataExpress.dsl.ETL._
import edu.chop.cbmi.dataExpress.dsl.stores.SqlDb
import edu.chop.cbmi.dataExpress.dataModels.RichOption._
import edu.chop.cbmi.dataExpress.test.util.presidents._


/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 1/30/12
 * Time: 1:10 PM
 * To change this template use File | Settings | File Templates.
 */


class GetFeatureSpec extends PresidentsFeatureSpecWithSourceTarget{

  //override val backend_test_type : KNOWN_SQL_BACKEND = POSTGRES()
  override val backend_test_type : KNOWN_SQL_BACKEND = MYSQL()

  val sql_statement = """select * from %s where %s='2'""".format(PRESIDENTS,source_backend.sqlDialect.quoteIdentifier("num_terms"))
  val bindable_statement = """select * from %s where %s= ?""".format(PRESIDENTS,source_backend.sqlDialect.quoteIdentifier("num_terms"))

  feature("The DSL get statement should allow the user to retrieve data from the source"){
    val source = "source"

    scenario("get is invoked with a normal query statement"){
      register store SqlDb(prop_file_source, schema) as source
      val table = get query sql_statement from source
      (0 /: table){ (i,r) => i+1} should equal(default_ttp_count)
      table foreach { row => row.num_terms.asu[Int] should equal(2)}
      ETL.cleanup()
    }

    scenario("get is invoked with a bindable query statement"){
      register store SqlDb(prop_file_source, schema) as source
      val table = get query bindable_statement using_bind_vars 1 from source
      (0 /: table){ (i,r) => i+1} should equal(default_otp_count)
      table foreach { row => row.num_terms.asu[Int] should equal(1)}

      val table2 = get query bindable_statement using_bind_vars 2 from source
      (0 /: table2){ (i,r) => i+1} should equal(default_ttp_count)
      table2 foreach { row => row.num_terms.asu[Int] should equal(2)}
      ETL.cleanup()
    }

    scenario("get is invoked using a table name"){
      register store SqlDb(prop_file_source, schema) as source
      val table = get table PRESIDENTS from source
      (0 /: table) { (i,r) => i+1} should equal(default_president_count)
      ETL.cleanup()
    }

  }

}
