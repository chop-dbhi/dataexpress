package edu.chop.cbmi.dataExpress.test.dataModels

import edu.chop.cbmi.dataExpress.dataModels.DataRow
import edu.chop.cbmi.dataExpress.dataModels.DataTable
import edu.chop.cbmi.dataExpress.dataModels.RichOption.optionToRichOption
import edu.chop.cbmi.dataExpress.dataModels.sql.SqlRelation
import edu.chop.cbmi.dataExpress.dataModels.sql.SqlTransform
import edu.chop.cbmi.dataExpress.test.util.presidents._
import edu.chop.cbmi.dataExpress.test.util.presidents.PresidentsSpecWithSourceTarget

class SqlTransformSpec extends PresidentsSpecWithSourceTarget {
	  //override val backend_test_type : KNOWN_SQL_BACKEND = POSTGRES()
     override val backend_test_type : KNOWN_SQL_BACKEND = MYSQL()
    
     describe("a SQL Transform") {
      it("should allow columns to be combined"){
    	  Given("a SQL Relation")
    	  val query = """select * from %s""".format(PRESIDENTS)
    	  val sr:SqlRelation[Any] = DataTable(source_backend, query)
    	  sr.hasColumn("first_name") should equal(true)
      
    	  val tf = SqlTransform(sr){dr:DataRow[_] => DataRow(("name", "%s %s".format(dr.first_name.asu[String], dr.last_name.asu[String])))}
    	  When("columnNames is called, the first transform should run to populate metadata")
    	  tf.columnNames(0) should be("name")
    	  tf.next.name.asu[String] should be("George Washington")
    	  When("hasNext is called, the next row should be obtained and held in memory")
    	  tf.hasNext
    	  When("next is called, the cursor is not advanced")
    	  tf.next.name.asu[String] should be("John Adams")
      }
     }
}