package edu.chop.cbmi.dataExpress.test.dsl

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 1/12/12
 * Time: 2:54 PM
 * To change this template use File | Settings | File Templates.
 */

import edu.chop.cbmi.dataExpress.dsl.ETL._
import edu.chop.cbmi.dataExpress.dataModels.RichOption._
import edu.chop.cbmi.dataExpress.dsl.{ETL}
import java.sql.Date
import edu.chop.cbmi.dataExpress.dsl.stores.{SqlDb}
import edu.chop.cbmi.dataExpress.dataModels.sql.{CharacterDataType, IntegerDataType}
import edu.chop.cbmi.dataExpress.dataModels.{DataRow, DataTable}
import edu.chop.cbmi.dataExpress.test.util.presidents._
import edu.chop.cbmi.dataExpress.test.util.presidents.{PresidentsSpecWithSourceTarget}
import edu.chop.cbmi.dataExpress.test.util.presidents.SQLStatements.potus_data_row



class CopySpec extends PresidentsSpecWithSourceTarget {

  //override val backend_test_type : KNOWN_SQL_BACKEND = POSTGRES()
  override val backend_test_type : KNOWN_SQL_BACKEND = MYSQL()

  //will be creating some extra tables
  val VICE_PRESIDENTS = add_known_table("vice_presidents")
  val PRESIDENTS_COPY = add_known_table("pres_copy")
  val TEST_SUBJECTS = add_known_table("test_subjects")
  val PC_ONE = add_known_table("pc_one")
  val PC_TWO = add_known_table("pc_two")
  val TTP_ONE = add_known_table("ttp_one")
  val UPPER_PRES_COPY = add_known_table("upper_pres_copy")
  val PRES_COLLAPSED_NAMES = add_known_table("pres_collapsed_names")
  val TTP_TWO = add_known_table("ttp_two")
  val TTP_THREE = add_known_table("ttp_three")
  val TTP_FOUR = add_known_table("ttp_four")
  val TTP_FIVE = add_known_table("ttp_five")

  describe("CopyFromTable"){
    it("should copy tables"){
      val source_db = "source_db"
      val target_db = "target_db"
      ETL.execute(on = true, do_cleanup = true){
    	  //was prop_file_target
        register store SqlDb(prop_file_target, schema) as target_db
        register store SqlDb(prop_file_source, schema) as source_db
          commit_on_success(target_db) {
            copy table PRESIDENTS from source_db to target_db create PC_ONE
          }
          AssertionOps.assertion_functions += ( () => query_and_count(PC_ONE, targetSchema = schema) should equal(default_president_count))
          BackendOps.add_table_name(target_backend, PC_ONE, schema)
          val init_pres_map = Map(1->potus_data_row(1), 2->potus_data_row(2), 3->potus_data_row(3), 4->potus_data_row(4))
          add_compare_table_assertion(PC_ONE, init_pres_map)

          commit_on_success(target_db){
            copy table PRESIDENTS from source_db change_column_names("first_name"->"FN", "last_name"->"LN") to target_db create PC_TWO
          }
          BackendOps.add_table_name(target_backend, PC_TWO, schema)
          add_compare_table_assertion(PC_TWO, init_pres_map, (query_row: DataRow[_], map_row : DataRow[_]) => {
            query_row.FN should equal(map_row.first_name)
            query_row.LN should equal(map_row.last_name)
            query_row.num_terms should equal(map_row.num_terms)
            query_row.dob.as[Date].toString should equal(map_row.dob.as[Date].toString)
            query_row.id should equal(map_row.id)
          })

          commit_on_success(target_db){
            copy table PRESIDENTS from source_db filter {row => row.num_terms.asu[Int] == 2} to target_db create TTP_ONE
          }
          BackendOps.add_table_name(target_backend, TTP_ONE, schema)
          val ttp_map = Map(1->potus_data_row(1), 3->potus_data_row (3), 4->potus_data_row (4))
          add_compare_table_assertion(TTP_ONE, ttp_map)

          commit_on_success(target_db){
            copy table PRESIDENTS from source_db transform {row =>
              val new_id = row.id.asu[Int] * 100
              new_row(
                "id" -> new_id,
                "first_name" -> row.first_name.asu[String].toUpperCase,
                "last_name" -> row.last_name.asu[String].toUpperCase,
                "num_terms" -> row.num_terms.asu[Int],
                "dob" -> row.dob.as[Date].getOrElse(null)
              )
            } to target_db create UPPER_PRES_COPY
          }

          BackendOps.add_table_name(target_backend, UPPER_PRES_COPY, schema)
          add_compare_table_assertion(UPPER_PRES_COPY, init_pres_map, (query_row: DataRow[_], map_row: DataRow[_]) => {
            query_row.first_name.asu[String] should equal(map_row.first_name.asu[String].toUpperCase)
            query_row.last_name.asu[String] should equal(map_row.last_name.asu[String].toUpperCase)
            query_row.num_terms should equal(map_row.num_terms)
            query_row.dob.as[Date].toString should equal(map_row.dob.as[Date].toString)
            query_row.id.asu[Int] should equal(map_row.id.asu[Int]*100)
          }, (i:Int)=>i/100)

          commit_on_success(target_db){
            copy table PRESIDENTS from source_db alter {table =>
              table set_data_types(IntegerDataType, CharacterDataType(20, false))
              table set_column_names("id", "name")
              table set_row_values {row =>
                val collapsed_name = row.last_name.asu[String] + ", " + row.first_name.asu[String]
                new_row(
                  "id" -> row.id.asu[Int],
                  "name" -> collapsed_name
                )
              }
            } to target_db create PRES_COLLAPSED_NAMES
          }
          BackendOps.add_table_name(target_backend, PRES_COLLAPSED_NAMES, schema)
          AssertionOps.assertion_functions += (() => {
            val t = DataTable(source_backend, """select * from %s.%s""".format(
              source_backend.sqlDialect.quoteIdentifier(schema.get),
              source_backend.sqlDialect.quoteIdentifier(PRES_COLLAPSED_NAMES)))
            t.hasColumn("num_terms") should equal(false)
            t.hasColumn("dob") should equal(false)
            t.hasColumn("first_name") should equal(false)
            t.hasColumn("last_name") should equal(false)
          })
          add_compare_table_assertion(PRES_COLLAPSED_NAMES, init_pres_map, (query_row: DataRow[_], map_row: DataRow[_]) => {
            "%s, %s".format(map_row.last_name.asu[String], map_row.first_name.asu[String]) should equal(query_row.name.asu[String])
            query_row.id should equal(map_row.id)
          })

          val sql_statement = """select * from %s.%s where %s='2'""".format(schema.get, PRESIDENTS,
            source_backend.sqlDialect.quoteIdentifier("num_terms"))

          commit_on_success(target_db) {
            copy query sql_statement from source_db to target_db create TTP_TWO
          }
          BackendOps.add_table_name(target_backend, TTP_TWO, schema)
          add_compare_table_assertion(TTP_TWO, ttp_map)

          val bindable_statement = """select * from %s.%s where %s= ?""".format(schema.get, PRESIDENTS,
            source_backend.sqlDialect.quoteIdentifier("num_terms"))
          commit_on_success(target_db) {copy query bindable_statement bind 2 from source_db to target_db create TTP_THREE}
          BackendOps.add_table_name(target_backend, TTP_THREE, schema)
          add_compare_table_assertion(TTP_THREE, ttp_map)

          commit_on_success(target_db) {
            copy table UPPER_PRES_COPY from source_db to target_db create TTP_FOUR
          }
          val rs = target_backend.executeQuery("select count(*) from %s.ttp_four".format(schema.get))
          rs.next()
          val num = rs.getInt(1)
          num should be > 0
          val sql_statement2 = """select * from %s.%s""".format(schema.get, PRESIDENTS)
          commit_on_success(target_db)
          {
            copy query sql_statement2 from source_db to target_db append TTP_FOUR
          }
          BackendOps.add_table_name(target_backend, TTP_FOUR, schema)
          AssertionOps.query_and_count(TTP_FOUR, target_backend, schema) should equal(2*default_president_count)

          commit_on_success(target_db) {
            copy table UPPER_PRES_COPY from source_db to target_db create TTP_FIVE
            copy table PRESIDENTS from source_db to target_db append TTP_FIVE
          }

          BackendOps.add_table_name(target_backend, TTP_FIVE, schema)
          AssertionOps.query_and_count(TTP_FIVE, target_backend, schema) should equal(2*default_president_count)
      } // This was here, but with refactoring seems like it shoulndn't be anymore: should equal(Left(true))

      AssertionOps.execute_assertions()
    }
  }
}
