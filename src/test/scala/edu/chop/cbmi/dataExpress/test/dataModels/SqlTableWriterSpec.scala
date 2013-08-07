package edu.chop.cbmi.dataExpress.test.dataModels

import java.util.Calendar
import edu.chop.cbmi.dataExpress.dataModels.{DataTable, DataRow}
import edu.chop.cbmi.dataExpress.dataModels.RichOption._
import edu.chop.cbmi.dataExpress.dataModels.sql.CharacterDataType
import edu.chop.cbmi.dataExpress.dataWriters.DataWriter
import edu.chop.cbmi.dataExpress.dataWriters.sql.SqlTableWriter
import edu.chop.cbmi.dataExpress.test.util.presidents._
import edu.chop.cbmi.dataExpress.test.util.{Functions}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 12/22/11
 * Time: 10:49 AM
 * To change this template use File | Settings | File Templates.
 */
@RunWith(classOf[JUnitRunner])
class SqlTableWriterSpec extends PresidentsSpecWithSourceTarget{

  //override val backend_test_type : KNOWN_SQL_BACKEND = POSTGRES()
  override val backend_test_type : KNOWN_SQL_BACKEND = MYSQL()

  //will be creating some new tables in the target
  val VICE_PRESIDENTS = "vice_presidents"
  BackendOps.add_table_name(target_backend, VICE_PRESIDENTS)

  val PRESIDENTS_COPY = "pres_copy"
  BackendOps.add_table_name(target_backend, PRESIDENTS_COPY)

  describe("A SqlTableWriter"){
    val dw = DataWriter(target_backend)
    it("should allow the insertion of data into a table in the db"){
      Given("a datarow with the appropriate number of arguments insert it into the table")

      val monroe = SQLStatements.potus_data_row(SQLStatements.MONROE)
      dw.insert_row(PRESIDENTS, monroe).operation_succeeded_? should equal(true)

      val jqa = SQLStatements.potus_data_row(SQLStatements.QUINCY_ADAMS)
      dw.insert_row(PRESIDENTS,jqa).operation_succeeded_? should equal(true)

      //not all columns are required
      val rr = DataRow("id"->7,"first_name"->"Ronald", "last_name"->"Reagan")
      dw.insert_row(PRESIDENTS,rr).operation_succeeded_? should equal(true)


      And("after commiting the backend the rows should appear in a new query")
      query_and_count(PRESIDENTS) should equal(7)

      Given("any DataTable whose DataRows have an appropriate number of arguments, insert all rows into the table")
      val gb = List(8,"George","Bush", 1)
      val bc = List(9,"William","Clinton",2)
      val sdt = DataTable(List("id", "first_name", "last_name", "num_terms"), gb, bc)
      dw.insert_rows(PRESIDENTS,sdt).operation_succeeded_? should equal(true)

      And("after committing the backend the rows of the table should be in a new query")
      query_and_count(PRESIDENTS) should equal(9)


      Given("a function mapping from a String to a T, insert a new row into the tables")
      dw.insert_row(PRESIDENTS,(name:String)=>{
        name match{
          case "id" => Some(10)
          case "first_name" => Some("Barack")
          case "last_name" => Some("Obama")
          case "num_terms" => Some(1)
          case "dob" => Some(Functions.sqlDateFrom("19610804"))
          case _ => None
        }
      })
      And("after comitting the backend the rows of the table should be in a new query")
      query_and_count(PRESIDENTS) should equal(10)


      And("it should allow updates of existing rows")
      Given("a new DataRow and a filter")
      val washington = DataRow("first_name"->"Bob")
      dw.update_row(PRESIDENTS, washington, "id"->1)
      query_and_count(PRESIDENTS) should equal(10)


      val gw = DataTable(source_backend, """select * from %s where %s='1'""".format(
        source_backend.sqlDialect.quoteIdentifier(PRESIDENTS),source_backend.sqlDialect.quoteIdentifier("id"))).next
      gw.first_name.asu[String] should equal("Bob")
      gw.last_name.asu[String] should equal("Washington")
      gw.id.asu[Int] should equal(1)
      gw.num_terms.asu[Int] should equal(2)


      And("a function mapping from a String to a T, insert a new row into the tables")
      dw.update_row(PRESIDENTS,"id"->2)((name:String)=>{
        name match{
          case "first_name" => Some("Johnie")
          case _ => None
        }
      })
      query_and_count(PRESIDENTS) should equal(10)

      val ja = DataTable(source_backend, """select * from %s where %s='2'""".format(
        target_backend.sqlDialect.quoteIdentifier(PRESIDENTS),source_backend.sqlDialect.quoteIdentifier("id"))).next
      ja.first_name.asu[String] should equal("Johnie")
      ja.last_name.asu[String] should equal("Adams")
      ja.num_terms.asu[Int] should equal(1)
      ja.id.asu[Int] should equal(2)


      And("it should allow creation of new tables")
      val biden = List("Joe","Biden")
      val cheney = List("Dick", "Cheney")
      val vps = DataTable(List("first_name","last_name"), biden, cheney)
      dw.insert_table(VICE_PRESIDENTS, List(CharacterDataType(20, false), CharacterDataType(20,false)),
        vps, SqlTableWriter.OVERWRITE_OPTION_DROP)
      BackendOps.add_table_name(target_backend, VICE_PRESIDENTS)
      query_and_count(VICE_PRESIDENTS) should equal(2)


      val the_pres_table = DataTable(source_backend, "SELECT * FROM %s".format(
        source_backend.sqlDialect.quoteIdentifier(PRESIDENTS)))
      val dts = the_pres_table.dataTypes
      dw.insert_table(PRESIDENTS_COPY, dts, the_pres_table, SqlTableWriter.OVERWRITE_OPTION_DROP)
      BackendOps.add_table_name(target_backend, PRESIDENTS_COPY)
      query_and_count(PRESIDENTS_COPY) should equal(10)
    }
  }
}
