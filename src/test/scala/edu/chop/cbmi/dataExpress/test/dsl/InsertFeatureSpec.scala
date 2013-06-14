package edu.chop.cbmi.dataExpress.test.dsl

import edu.chop.cbmi.dataExpress.dsl.ETL
import edu.chop.cbmi.dataExpress.dsl.ETL._
import edu.chop.cbmi.dataExpress.dsl.stores.{FileStore, SqlDb}
import edu.chop.cbmi.dataExpress.dataModels.RichOption._
import edu.chop.cbmi.dataExpress.test.util.Functions
import edu.chop.cbmi.dataExpress.test.util.presidents._
import java.sql.Date
import edu.chop.cbmi.dataExpress.dataModels.{DataRow, DataTable}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import edu.chop.cbmi.dataExpress.backends.file.{HeaderRowColumnNames, TextFileBackend}
import java.io.File


/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 2/1/12
 * Time: 8:59 AM
 * To change this template use File | Settings | File Templates.
 */

@RunWith(classOf[JUnitRunner])
class InsertFeatureSpec extends PresidentsFeatureSpecWithSourceTarget{

  //override val backend_test_type: KNOWN_SQL_BACKEND = POSTGRES()
  override val backend_test_type : KNOWN_SQL_BACKEND = MYSQL()
  lazy val fileOne = new File("./output/InsertFileSpec1.dat")
  override def before_func(){
    super.before_func()
    if(fileOne.exists())fileOne.delete()
    //create a file to read
  }

  override def after_func(){
    super.after_func()
    if(fileOne.exists())fileOne.delete()
  }

  feature("The DSL insert statement should allow the user to insert rows into an existing SQL table"){
    val source = "source"
    val bindable_statement = """select * from %s where %s= ?""".format(PRESIDENTS, source_backend.sqlDialect.quoteIdentifier("id"))

    scenario("insert a complete row into the public schema") {

      ETL.execute(true, true) {
        register store SqlDb(prop_file_source, schema) as source
        commit_on_success(source) {
          insert row new_row(
            "id" -> (default_president_count + 1),
            "first_name" -> "James",
            "last_name" -> "Monroe",
            "num_terms" -> 2,
            "dob" -> Functions.sqlDateFrom("17580428")
          ) to source append PRESIDENTS

          AssertionOps.assertion_functions += (() => query_and_count(PRESIDENTS, false) should equal(default_president_count + 1))
          AssertionOps.assertion_functions += (() => {
            val monroe = DataTable(source_backend, bindable_statement, Seq(Some(default_president_count + 1))).next
            monroe.first_name.asu[String] should equal("James")
            monroe.last_name.asu[String] should equal("Monroe")
            monroe.num_terms.asu[Int] should equal(2)
            monroe.dob.asu[Date].toString should equal(Functions.sqlDateFrom("17580428").toString)
          })
        }
      } should equal(Left(true))
      AssertionOps.execute_assertions()
    }
  }

  feature("The DSL insert statement should allow the user to insert rows into a file"){

    lazy val fs = "fileStore"

    scenario("insert a complete row into the file"){
      ETL.execute(true, true){

        val tfb = TextFileBackend(fileOne, PresMarshaller())

        tfb.writeHeader(DataRow(List("id","first_name","last_name","num_terms", "dob").map{cn=>(cn,cn)}: _*))

        register store FileStore(tfb, HeaderRowColumnNames(fileOne)) as fs

        insert row new_row(
          "id" -> (default_president_count + 1),
          "first_name" -> "James",
          "last_name" -> "Monroe",
          "num_terms" -> 2,
          "dob" -> Functions.sqlDateFrom("17580428")
        ) to fs append

        true
      }should equal(Left(true))
    }

  }
}
