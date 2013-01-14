package edu.chop.cbmi.dataExpress.test.dataModels

import edu.chop.cbmi.dataExpress.dataModels.{DataRow, DataTable}
import edu.chop.cbmi.dataExpress.dataModels.RichOption._
import java.sql.Date
import java.util.GregorianCalendar
import edu.chop.cbmi.dataExpress.exceptions.ColumnDoesNotExist
import edu.chop.cbmi.dataExpress.test.util.presidents._
import org.scalatest.{GivenWhenThen, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest._
import edu.chop.cbmi.dataExpress.dataModels.sql.SqlRelation


/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 12/2/11
 * Time: 10:39 AM
 * To change this template use File | Settings | File Templates.
 */

class SqlRelationSpec extends PresidentsSpecWithSourceTarget{

  //override val backend_test_type : KNOWN_SQL_BACKEND = POSTGRES()
  override val backend_test_type : KNOWN_SQL_BACKEND = MYSQL()

  describe("A SqlRelation") {
    it("should result from a call to DataTable") {
      given("a query statement and a datastore")
      val query = """select * from %s""".format(PRESIDENTS)
      val sr:SqlRelation[Any] = DataTable(source_backend, query)
     
      and("the table should have column names that match the table meta information")
      sr.hasColumn("first_name") should equal(true)
      sr.hasColumn("middle_name") should equal(false)

      when("accessed via next it should emit a new DataRow and tail it should give a DataRow and a new Iterable[DataRow[Option[Any]]] respectively")
      val row1 = sr.next
      row1.id.as[Int] should equal(Some(1))
      row1.id.asu[Int] should equal(1)

      row1.first_name.asu[String] should equal("George")
      row1.first_name.as[String] should equal(Some("George"))

      row1.last_name.as[String] should equal(Some("Washington"))
      row1.last_name.asu[String] should equal("Washington")

      row1.num_terms.as[Int] should equal(Some(2))
      row1.num_terms.asu[Int] should equal(2)

      val calender  = new GregorianCalendar(1732,1,22)
      row1.dob.asu[Date] should equal(new Date(calender.getTimeInMillis))

      //also test accessor methods
      val row2 = sr.next
      row2(0).asu[Int] should equal(2)
      row2(1).asu[String] should equal("John")
      row2("last_name").asu[String] should equal("Adams")
      row2("num_terms").asu[Int] should equal(1)
      row2.dob should equal(None)
      //This is an iterator now, so this is no longer valid
//      and("it should be allowed to peform mulitple iterations over the table")
//      val sum_terms = (sr.num_terms/:sr)(_ + _.num_terms)
//      sum_terms should equal(7)

//      and("it should allow using the VIEW method to avoid multiple calls to the database and faster computation")
//      val ids_view = sr.view.map((dr:DataRow[Option[Any]])=>alpha_from_int(dr.id.asu[Int]-1)).map((s:String)=>int_from_alpha(s)+1)
//      val id_force = ids_view.force
//      val id_sum =(id_force.head /: id_force.tail){_+_}
//      id_sum should equal(10)

      when("accessed using the col method, it should return a SqlRelationColumn[Option[_]]")
      val term_lengths = sr.col("num_terms")
     // ((term_lengths.next.asu[Int] /: term_lengths){_ + _.asu[Int]}) should equal(7)
      
      //Since replacing with Iterator destroys the View support, this test may not be valid anymore
      and("it should be reusable as an iterator and support the VIEW method")
      (term_lengths.filter((o:Option[_])=>o.as[Int].get>1).map((o:Option[_])=>o.asu[Int]).size) should equal(3)

      when("accessed using the col_as[G] method it should return SqlRelationColumn[Option[G]]")
      val opt_int_ids = sr.col_as[Int]("id")
     // ((opt_int_ids.next.get /: opt_int_ids)(_+_.get)) should equal(10)

      when("accessed using the col_asu[G] method it should return SqlRelationColumn[G]")
      val int_ids = sr.col_asu[Int]("id")
     // ((int_ids.next /: int_ids)(_+_)) should equal(10)

      when("trying to access columns that don't exist should throw ColumnDoesNotExist for all access methods")
      intercept[ColumnDoesNotExist]{sr.bad_name}
      intercept[ColumnDoesNotExist]{sr.col("bad_name")}
      intercept[ColumnDoesNotExist]{sr.col_as[Any]("bad_name")}
      intercept[ColumnDoesNotExist]{sr.col_asu[Any]("bad_name")}

    }
  }

  private def alpha_from_int(i: Int) = {
    val alphabet = "abcdefghijklmnopqrstuvwxyz"
    "a" * (i / 26) + alphabet(i % 26)
  }

  private def int_from_alpha(s:String) = {
    val alphabet = "abcdefghijklmnopqrstuvwxyz"
    alphabet.indexOf(s)
  }


}