package edu.chop.cbmi.dataExpress.test.util.presidents

import edu.chop.cbmi.dataExpress.test.util.Functions.sqlDateFrom
import java.sql.Date
import edu.chop.cbmi.dataExpress.backends.{PostgresBackend, MySqlBackend, SqlBackend}
import edu.chop.cbmi.dataExpress.dataModels.DataRow

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 3/16/12
 * Time: 12:51 PM
 * To change this template use File | Settings | File Templates.
 */

class KNOWN_SQL_BACKEND

case class POSTGRES() extends KNOWN_SQL_BACKEND

case class MYSQL() extends KNOWN_SQL_BACKEND

object KNOWN_SQL_BACKEND{
  def backend_type(backend : SqlBackend) = backend match{
    case (be:MySqlBackend) => Some(new MYSQL)
    case (be:PostgresBackend) =>Some(new POSTGRES)
    case _=> None
  }
}

object SQLStatements {
  val WASHINGTON = 1
  val ADAMS_JOHN = 2
  val JEFFERSON = 3
  val MADISON  = 4
  val MONROE = 5
  val QUINCY_ADAMS = 6
  val CLINTON = 42

  val PRESIDENTS = "presidents"
  val TWO_TERM_PRESIDENTS = "two_term_presidents"
  val ONE_TERM_PRESIDENTS = "one_term_presidents"

  def formated_date_string(president: Int, backend_type: KNOWN_SQL_BACKEND) = backend_type match {
    case (bs: MYSQL) => president match {
      case 1 => "'1732-02-22'"
      case 3 => "'1743-04-13'"
      case 4 => "'1751-03-16'"
      case 5 => "'1758-04-28'"
      case _ => "NULL"
    }
    case (bs: POSTGRES) => president match {
      case 1 => "'Feb-22-1732'"
      case 3 => "'Apr-13-1743'"
      case 4 => "'Mar-16-1751'"
      case 5 => "'Apr-28-1758'"
      case _ => "NULL"
    }
  }

  def potus(president : Int, id : Option[Int] = None) = {
    val date = president match{
      case 1 => sqlDateFrom("17320222")
      case 3 => sqlDateFrom("17430413")
      case 4 => sqlDateFrom("17510316")
      case 5 => sqlDateFrom("17580428")
      case _ => null
    }
    val pid = id match{
      case None =>president
      case Some(i) => i
    }
    president match {
      case 1 => (pid,"George", "Washington", 2, date)
      case 2 => (pid,"John", "Adams", 1, date)
      case 3 => (pid, "Thomas", "Jefferson", 2, date)
      case 4 => (pid, "James", "Madison", 2, date)
      case 5 => (pid, "James", "Monroe", 2, date)
      case 6 => (pid, "John", "Quincy Adams", 1, date)
      case 42 => (pid, "William", "Clinton",2,date)
      case _ => throw new Exception("Uknown POTUS id")
    }
  }

  def potus_data_row(president : Int, id : Option[Int] = None) = {
    val p = potus(president, id)
    p._5 match{
      case null => (DataRow(List("id","first_name","last_name","num_terms", "dob"))(
            List(Some(p._1), Some(p._2), Some(p._3), Some(p._4), None)))
      case _ => (DataRow(List("id","first_name","last_name","num_terms","dob"))
            (List(Some(p._1), Some(p._2), Some(p._3), Some(p._4), Some(p._5))))
    }
  }

  def default_president_list() = {
    List(potus(1), potus(2), potus(3), potus(4))
  }

  def drop_table(table_name: String, sqlbackend: Option[SqlBackend]) = sqlbackend match {
    case None => """DROP TABLE IF EXISTS %s""".format(table_name)
    case Some(sbe) => """DROP TABLE IF EXISTS %s""".format(sbe.sqlDialect.quoteIdentifier(table_name))
  }

  def create_president_table(backend_type : KNOWN_SQL_BACKEND, schema : String) = backend_type match{
    case (bs:POSTGRES) =>"""CREATE TABLE "%s"."presidents"
                                (id serial NOT NULL,
                                 first_name character varying(20),
                                 last_name character varying(50),
                                 num_terms integer NOT NULL DEFAULT 1,
                                 dob date,
                                 CONSTRAINT pk PRIMARY KEY (id)
                                )
                                WITH (
                                  OIDS=FALSE
                                 );""".format(schema)
    case (bs:MYSQL) => """CREATE TABLE `%s`.`presidents` (
          	`id` SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
          	`first_name` varchar(20),
          	`last_name` varchar(50),
          	`num_terms` SMALLINT UNSIGNED NOT NULL DEFAULT '1',
          	`dob` date,
          	PRIMARY KEY (`id`)
          );""".format(schema)
  }

  def insert_president_values(backend_type : KNOWN_SQL_BACKEND,
        presidents : List[(Int,String,String,Int,Date)],
        insert_prefix : String = """INSERT INTO presidents(id, first_name, last_name, num_terms, dob) VALUES %s""") = {
    val values = (""/:presidents) {(s,t) => s + "(%s,'%s','%s',%s,%s),".format(t._1.toString,t._2,t._3,t._4.toString,formated_date_string(t._1,backend_type))}
    insert_prefix.format(values.substring(0, values.length-1))
  }
}
