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

  def add_table_name(backend : SqlBackend, table_name : String, schema: Option[String]) = {
    val fullyQualifiedTableName = schema.get match {
      case "" => s"${backend.sqlDialect.quoteIdentifier(table_name)}"
      case _ => s"${backend.sqlDialect.quoteIdentifier(schema.get)}.${backend.sqlDialect.quoteIdentifier(table_name)}"
    }
    if(KNOWN_TABLES.contains(backend)) KNOWN_TABLES(backend) = fullyQualifiedTableName :: KNOWN_TABLES(backend)
    else KNOWN_TABLES(backend) = List(fullyQualifiedTableName)
  }

  def execute_drop_table(table_name : String, backend : SqlBackend, schema: Option[String]) = {
    backend.execute(SQLStatements.drop_table(table_name, Some(backend), schema))
    backend.commit
  }

  private def drop_known_backend_tables(key: SqlBackend) = KNOWN_TABLES.get(key) match {
    case Some(l) => l foreach {table =>
          execute_drop_table(table, key, None) //Pass None because we keep fully qualified table names in the list
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

  def query_and_count(table_name: String, backend : SqlBackend, schema: Option[String]) = {
    val fullyQualifiedTableName = schema.get match {
      case "" => s"${backend.sqlDialect.quoteIdentifier(table_name)}"
      case _ => s"${backend.sqlDialect.quoteIdentifier(schema.get)}.${backend.sqlDialect.quoteIdentifier(table_name)}"
    }

    DataTable(backend, """select * from %s""".format(fullyQualifiedTableName)).length
  }

  def execute_assertions() = {
    assertion_functions foreach {f => f.apply()}
  }

}
