package edu.chop.cbmi.dataExpress.test.util.presidents

import edu.chop.cbmi.dataExpress.backends.SqlBackend
import edu.chop.cbmi.dataExpress.dataModels.sql.SqlRelation
import edu.chop.cbmi.dataExpress.dataModels.{DataTable}

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 3/16/12
 * Time: 2:27 PM
 * To change this template use File | Settings | File Templates.
 */

object BackendOps {

  val KNOWN_TABLES = scala.collection.mutable.Map.empty[SqlBackend, List[String]]

  def add_table_name(backend : SqlBackend, table_name : String) = {
    if(KNOWN_TABLES.contains(backend))KNOWN_TABLES(backend) = table_name :: KNOWN_TABLES(backend)
    else KNOWN_TABLES(backend) = List(table_name)
  }

  def execute_drop_table(table_name : String, backend : SqlBackend) = {
    backend.execute(SQLStatements.drop_table(table_name, Some(backend)))
    backend.commit
  }

  private def drop_known_backend_tables(key: SqlBackend) = KNOWN_TABLES.get(key) match {
    case Some(l) => l foreach {table =>
          execute_drop_table(table, key)
    }
    case None => {}//nothing to do if there are no tables to drop
  }

  def drop_known_tables(backend: Option[SqlBackend] = None) = {
    backend match {
      case None => KNOWN_TABLES.keys foreach {
        key =>
          drop_known_backend_tables(key)
      }
      case Some(be) =>{
        drop_known_backend_tables(be)
      }
    }

  }

}

object TableOps{
  def convert_to_in_mem_table(sql_relation : SqlRelation[_]) = {
    DataTable(sql_relation.columnNames, sql_relation.toSeq)

  }
}

object AssertionOps {
  val assertion_functions = scala.collection.mutable.ListBuffer.empty[() => Any]

  def query_and_count(table_name: String, backend : SqlBackend) = {
      (0 /: DataTable(backend, """select * from %s""".format(backend.sqlDialect.quoteIdentifier(table_name)))) { (i,r)=> i +1 }
  }

  def execute_assertions() = {
    assertion_functions foreach {f => f.apply()}
  }

}
