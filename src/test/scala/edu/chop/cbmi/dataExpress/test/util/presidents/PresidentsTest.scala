package edu.chop.cbmi.dataExpress.test.util.presidents

import edu.chop.cbmi.dataExpress.backends.SqlBackend
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{Spec, GivenWhenThen, FunSpec, BeforeAndAfter}
import edu.chop.cbmi.dataExpress.dataModels.RichOption._
import edu.chop.cbmi.dataExpress.dataModels.{DataTable, DataRow}
import java.sql.Date
import edu.chop.cbmi.dataExpress.test.util.TestProps
import org.scalatest.FeatureSpec

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 1/27/12
 * Time: 1:44 PM
 * To change this template use File | Settings | File Templates.
 */

trait PresidentsTest {
  val perform_before_steps = true
  val perform_after_steps = true
  val PRESIDENTS = SQLStatements.PRESIDENTS
  val TWO_TERM_PRESIDENTS = SQLStatements.TWO_TERM_PRESIDENTS
  val ONE_TERM_PRESIDENTS = SQLStatements.ONE_TERM_PRESIDENTS

  def default_president_count = SQLStatements.default_president_list().length

  def default_ttp_count = {
    val t = SQLStatements.default_president_list() filter {
      l =>
        l._4 == 2
    }
    t.length
  }

  def default_otp_count = {
    val t = SQLStatements.default_president_list() filter {
      l =>
        l._4 == 1
    }
    t.length
  }

  def create_default_presidents_table(backend: SqlBackend, schema: Option[String]) = {
    backend.execute(SQLStatements.drop_table(PRESIDENTS, Some(backend)))
    val backend_type = KNOWN_SQL_BACKEND.backend_type(backend) match {
      case Some(bet) => bet
      case None => throw new Exception("Unsupported backend")
    }
    val create_statement = SQLStatements.create_president_table(backend_type, schema.getOrElse(null))
    backend.execute(create_statement)
    val insert_statement = SQLStatements.insert_president_values(backend_type, SQLStatements.default_president_list())
    backend.execute(insert_statement)
    backend.commit()
    BackendOps.add_table_name(backend, PRESIDENTS)
  }

  def NewTestPropsBackend(backend_type: KNOWN_SQL_BACKEND): SqlBackend = backend_type match {
    case (bet: POSTGRES) => TestProps.postGresTestDB()
    case (bet: MYSQL) => TestProps.mySqlTestDB()
    case _ => throw new Exception("Unsupported Backend Selection")
  }

  def before_func(): Unit = {

  }

  def after_func(): Unit = {

  }

}

abstract class PresidentsFeatureSpec extends FeatureSpec with GivenWhenThen with ShouldMatchers with BeforeAndAfter with PresidentsTest {

  before {
    if (perform_before_steps) {
      BackendOps.drop_known_tables()
      BackendOps.KNOWN_TABLES.clear()
      AssertionOps.assertion_functions.clear()
      before_func()
    }
  }

  after {
    if (perform_after_steps) {
      after_func()
      BackendOps.drop_known_tables()
      BackendOps.KNOWN_TABLES.clear()
      AssertionOps.assertion_functions.clear()
    }
  }
}

abstract class PresidentsFeatureSpecWithSourceTarget extends PresidentsFeatureSpec {
  val backend_test_type: KNOWN_SQL_BACKEND

  lazy val schema = backend_test_type match {
    case (x: POSTGRES) => Option("public")
    case (y: MYSQL) => Option("qe10")
    case (z) => throw new Exception("Unknown Backend Test Type")
  }

  lazy val source_backend = NewTestPropsBackend(backend_test_type)
  lazy val target_backend = NewTestPropsBackend(backend_test_type)

  lazy val prop_file_source = backend_test_type match {
    case (x: POSTGRES) => TestProps.getDbPropFilePath("postgres")
    case (y: MYSQL) => TestProps.getDbPropFilePath("mysql")
  }

  lazy val prop_file_target = backend_test_type match {
    case (x: POSTGRES) => TestProps.getDbPropFilePath("postgres")
    case (y: MYSQL) => TestProps.getDbPropFilePath("mysql")
  }

  override def before_func() = {
    create_default_presidents_table(target_backend, schema)
  }

  override def after_func() = {
    source_backend.close()
  }

  def query_and_count(table_name: String, commit_target: Boolean = true, target: SqlBackend = target_backend,
                      source: SqlBackend = source_backend) = {
    if (commit_target) target.commit
    AssertionOps.query_and_count(table_name, source)
  }

  def add_known_table(table_name: String, backend: SqlBackend = target_backend) = {
    BackendOps.add_table_name(backend, table_name)
    table_name
  }

  def standard_potus_compare(r1: DataRow[_], r2: DataRow[_]) = {
    r1.first_name should equal(r2.first_name)
    r1.last_name should equal(r2.last_name)
    r1.num_terms should equal(r2.num_terms)
    r1.dob.as[Date].toString should equal(r2.dob.as[Date].toString)
    r1.id should equal(r2.id)
  }

  def add_compare_table_assertion(table_name: String, compare_to: Map[Int, DataRow[_]],
                                  comparator: (DataRow[_], DataRow[_]) => Any = standard_potus_compare _,
                                  map_id_func: (Int) => Int = (i: Int) => i,
                                  source: SqlBackend = source_backend) = {
    AssertionOps.assertion_functions += (() => {
      //NOTE: WITH THE CHANGE OF DATATABLE TO AN ITERATOR, TAKING THE LENGTH CAUSES THE TABLE TO BE EMPTY
      val table_length = DataTable(source, """select * from %s.%s""".format(
        source.sqlDialect.quoteIdentifier(schema.getOrElse(null)), source.sqlDialect.quoteIdentifier(table_name))).length
      if(table_length != compare_to.size)fail(s"len(DataTable(${table_name})==${table_length} != len(compare_to)=${compare_to.size}")
      val table = DataTable(source, """select * from %s.%s""".format(
        source.sqlDialect.quoteIdentifier(schema.getOrElse(null)), source.sqlDialect.quoteIdentifier(table_name)))

      table foreach {
        row =>
          compare_to.get(map_id_func(row.id.asu[Int])) match {
            case Some(other_row) => comparator(row, other_row)
            case None => fail(s"could not find key ${map_id_func(row.id.asu[Int])}")
          }
      }
    })
  }
}

abstract class PresidentsSpec extends FunSpec with GivenWhenThen with ShouldMatchers with BeforeAndAfter with PresidentsTest {

  before {
    if (perform_before_steps) {
      BackendOps.drop_known_tables()
      BackendOps.KNOWN_TABLES.clear()
      AssertionOps.assertion_functions.clear()
      before_func()
    }
  }

  after {
    if (perform_after_steps) {
      after_func()
      BackendOps.drop_known_tables()
      BackendOps.KNOWN_TABLES.keys foreach {
        backend => backend.close()
      }
      BackendOps.KNOWN_TABLES.clear()
      AssertionOps.assertion_functions.clear()
    }
  }
}

abstract class PresidentsSpecWithSourceTarget extends PresidentsSpec {
  val backend_test_type: KNOWN_SQL_BACKEND

  lazy val schema = backend_test_type match {
    case (x: POSTGRES) => Option("public")
    case (y: MYSQL) => Option("qe10")
    case (z) => throw new Exception("Unknown Backend Test Type")
  }

  lazy val source_backend = NewTestPropsBackend(backend_test_type)
  lazy val target_backend = NewTestPropsBackend(backend_test_type)

  lazy val prop_file_source = backend_test_type match {
    case (x: POSTGRES) => TestProps.getDbPropFilePath("postgres")
    case (y: MYSQL) => TestProps.getDbPropFilePath("mysql")
  }

  lazy val prop_file_target = backend_test_type match {
    case (x: POSTGRES) => TestProps.getDbPropFilePath("postgres")
    case (y: MYSQL) => TestProps.getDbPropFilePath("mysql")
  }

  override def before_func() = {
    create_default_presidents_table(target_backend, schema)
  }

  override def after_func() = {
    source_backend.close()
  }

  def query_and_count(table_name: String, commit_target: Boolean = true, target: SqlBackend = target_backend,
                      source: SqlBackend = source_backend) = {
    if (commit_target) target.commit
    AssertionOps.query_and_count(table_name, source)
  }

  def add_known_table(table_name: String, backend: SqlBackend = target_backend) = {
    BackendOps.add_table_name(backend, table_name)
    table_name
  }

  def standard_potus_compare(r1: DataRow[_], r2: DataRow[_]) = {
    r1.first_name should equal(r2.first_name)
    r1.last_name should equal(r2.last_name)
    r1.num_terms should equal(r2.num_terms)
    r1.dob.as[Date].toString should equal(r2.dob.as[Date].toString)
    r1.id should equal(r2.id)
  }

  def add_compare_table_assertion(table_name: String, compare_to: Map[Int, DataRow[_]],
                                  comparator: (DataRow[_], DataRow[_]) => Any = standard_potus_compare _,
                                  map_id_func: (Int) => Int = (i: Int) => i,
                                  source: SqlBackend = source_backend) = {
    AssertionOps.assertion_functions += (() => {
      //NOTE: WITH THE CHANGE OF DATATABLE TO AN ITERATOR, TAKING THE LENGTH CAUSES THE TABLE TO BE EMPTY
      val table_length = DataTable(source, """select * from %s.%s""".format(
        source.sqlDialect.quoteIdentifier(schema.getOrElse(null)), source.sqlDialect.quoteIdentifier(table_name))).length
      if(table_length != compare_to.size)fail(s"len(DataTable(${table_name})==${table_length} != len(compare_to)=${compare_to.size}")
      val table = DataTable(source, """select * from %s.%s""".format(
        source.sqlDialect.quoteIdentifier(schema.getOrElse(null)), source.sqlDialect.quoteIdentifier(table_name)))

       table foreach {
        row =>
          compare_to.get(map_id_func(row.id.asu[Int])) match {
            case Some(other_row) => comparator(row, other_row)
            case None => fail(s"could not find key ${map_id_func(row.id.asu[Int])}")
          }
      }
    })
  }
}