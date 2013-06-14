package edu.chop.cbmi.dataExpress.dataModels.sql

import edu.chop.cbmi.dataExpress.dataModels.RichOption._
import edu.chop.cbmi.dataExpress.exceptions.ColumnDoesNotExist
import edu.chop.cbmi.dataExpress.backends.SqlBackend
import collection.Seq
import edu.chop.cbmi.dataExpress.dataModels._
import java.sql.{ResultSetMetaData, ResultSet}
import scala.language.dynamics
import scala.reflect.Manifest


//TODO: Maybe use this?
//case class SqlQuery(query: String)(bindVars:Seq[Option[_]] = Seq())(backend: SqlBackend) {
//  
//}

/**
 * A [[edu.chop.cbmi.dataExpress.dataModels.DataTable]] that represents a SQL relation (either a table or
 * the complete result of a query).
 * 
 */

trait SqlMetadata extends Metadata {
  val resultSetMetadata:java.sql.ResultSetMetaData
}

sealed case class SqlRelation[+T] private[dataModels](query:String, bindVars:Seq[Option[_]],  backend:SqlBackend) extends DataTable[T] with SqlMetadata{
  override lazy val columnNames:Seq[String] = (1 to columnCount).map{resultSetMetadata.getColumnLabel(_)}.toSeq
	override lazy val dataTypes:Seq[DataType] = backend.sqlDialect.mapDataTypes(columnNames,resultSetMetadata)
	override lazy val columnCount:Int = resultSetMetadata.getColumnCount()
	override lazy val resultSetMetadata = resultSet.getMetaData()
	lazy private val resultSet = backend.executeQuery(query, bindVars)
	lazy private val iterator = SqlRelationIterator(this)

  
  override def next() = iterator.next()
  
  override def hasNext = iterator.hasNext

  //TODO: This needs to be in a SQL dialect if at all possible
  def sub_query(name : String) = """SELECT  %s FROM (%s) AS %s""".format(name, query,name)


  override def col(name:String) = {
	  if(hasColumn(name)) SqlRelation(sub_query(name), bindVars, backend).map{r:DataRow[T] => r(0)}
      else throw ColumnDoesNotExist(name)
	}

  override def col_as[G](name: String)(implicit m: Manifest[G]): Iterator[Option[G]] = {
    if(hasColumn(name)) SqlRelation(sub_query(name), bindVars, backend).map{r:DataRow[G] => r(0).as[G]}
    else throw ColumnDoesNotExist(name)
  }

  override def col_asu[G](name: String)(implicit m: Manifest[G]): Iterator[Option[G]] = {
    if(hasColumn(name)) SqlRelation(sub_query(name), bindVars, backend).map{r:DataRow[_] => Some(r(0).asu[G])}
    else throw ColumnDoesNotExist(name)
	}

  override def filterRows(f:DataRow[_] => Boolean): DataTable[T] = {
    this.filterRows(f)
  }
 
  case class SqlRelationIterator[+T] (sqlRelation : SqlRelation[T]) extends Iterator[DataRow[T]] {

    lazy private val column_names = sqlRelation.columnNames
    lazy private val column_count = sqlRelation.columnCount
    protected var cursor_advanced = false
    protected var more_rows = false
    
    
  override def hasNext = {
    if (!cursor_advanced) {
      cursor_advanced = true
      more_rows = sqlRelation.resultSet.next()
    }
    more_rows
  }

  override def next() = {
    if (cursor_advanced) cursor_advanced = false
    else more_rows = sqlRelation.resultSet.next()
    //Some databases will complain if a result set is not properly closed, need to generate the next set of values
    //then close the result set.
    val nextRow = generate_next()
    if(!hasNext) {
      sqlRelation.resultSet.close()
    }
    nextRow
  }


  def next_item_in_column(i: Int) = sqlRelation.resultSetMetadata.getColumnType(i) match {
    //Postgres Boolean values such as 't' were having issues
    //because they are mapped to java.sql.Types.BIT
    //TODO: this code needs to be moved out of here
    case java.sql.Types.BIT => sqlRelation.resultSet.getBoolean(i)
    case _ => sqlRelation.resultSet.getObject(i)
  }
    
    
    def generate_next():DataRow[T] = {
      val row = (1 to column_count) map (next_item_in_column(_))
      val dr = DataRow(column_names)(row map ((x: Any) => if (x == null) None else Some(x.asInstanceOf[T])))
      dr
    }
  }

}


