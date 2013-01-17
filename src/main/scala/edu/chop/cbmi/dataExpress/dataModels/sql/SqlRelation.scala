package edu.chop.cbmi.dataExpress.dataModels.sql

import edu.chop.cbmi.dataExpress.dataModels.RichOption._
import edu.chop.cbmi.dataExpress.exceptions.ColumnDoesNotExist
import edu.chop.cbmi.dataExpress.backends.SqlBackend
import collection.Seq
import edu.chop.cbmi.dataExpress.dataModels.{DataType, DataRow, ColumnNameGenerator, DataTable}
import java.sql.{ResultSetMetaData, ResultSet}
import scala.language.dynamics

/**
 * Wrapper for queries that produce a JDBC ResultSet
 * 
 * @param dataStore A [[edu.chop.cbmi.dataExpress.backends.SqlBackend]] with an open connection, ready for queries
 * @param query A string representation of a SQL query, optionally with placeholder values for bind variables
 * @param bindVars An optional set of values to be bound when making a prepared statement
 * 
 */
case class SqlQueryPackage(val dataStore : SqlBackend, val query : String, val bindVars : Seq[Option[_]]) extends ColumnNameGenerator{
  private var _resultSet : Option[ResultSet] = None
  private var _meta : Option[ResultSetMetaData] = None
  private var _columnCount : Option[Int] = None
  private var _columnNames : Option[Seq[String]] = None
  private var _dataTypes : Option[Seq[DataType]] = None

  def resultSet() : ResultSet = {
    _resultSet match{
      case Some(rs) => rs
      case _ =>{
        val rs = dataStore.executeQuery(query, bindVars)
        _resultSet = Some(rs)
        rs
      }
    }
  }

  def meta() : ResultSetMetaData = {
    _meta match{
      case Some(m) => m
      case _ => {
        val m = resultSet.getMetaData
        _meta = Some(m)
        m
      }
    }
  }

  def columnCount() : Int = {
    _columnCount match {
      case Some(cc) => cc
      case _ => {
       val cc = meta.getColumnCount
        _columnCount = Some(cc)
        cc
      }
    }
  }

  def columnNames() : Seq[String] = {
    _columnNames match{
      case Some(cn) => cn
      case _ =>{
        val cn = (1 to columnCount) map {(i:Int)=>meta.getColumnLabel(i)}
        _columnNames = Some(cn)
        cn
      }
    }
  }

  def dataTypes() : Seq[DataType] = {
    val m = meta
    val cn = columnNames
    if(!dataStore.SUPPORTS_MULT_RS){
      _resultSet match{
        case Some(rs) => rs.close()
        case _ => {}
      }
      _resultSet = None
    }
    //val dt = dataStore.sqlDialect.mapDataTypes(cn,m)
    //dt
    dataStore.sqlDialect.mapDataTypes(cn,m)
  }

  def generate_column_names() = columnNames
}

/**
 * A [[edu.chop.cbmi.dataExpress.dataModels.DataTable]] that represents a SQL relation (either a table or
 * the complete result of a query).
 * 
 */
sealed case class SqlRelation[+T] private[dataModels](private val sql_query_package : SqlQueryPackage)
  extends DataTable[T](sql_query_package) {

  lazy private val sqlMeta = sql_query_package.meta
  lazy private val columnCount = sql_query_package.columnCount
  lazy val dataTypes: Seq[DataType] = sql_query_package.dataTypes
  lazy private val query_package = SqlQueryPackage(sql_query_package.dataStore, sql_query_package.query, sql_query_package.bindVars)
  lazy private val iterator = SqlRelationIterator(query_package)
  
  override def next() = iterator.next()
  
  override def hasNext = iterator.hasNext

  //TODO: This needs to be in a SQL dialect if at all possible
  private def sub_query(name : String) =
    """SELECT  """ + name + """ FROM (""" + sql_query_package.query + """) AS """ + name

  override def col(name:String) = if(hasColumn(name)){
    def f(item:Any) = if(item==null) None else Some(item.asInstanceOf[T])
    SqlRelationColumn[Option[T]](SqlQueryPackage(sql_query_package.dataStore, sub_query(name), sql_query_package.bindVars), f _)
  }else throw ColumnDoesNotExist(name)

  override def col_as[G](name: String)(implicit m: Manifest[G]) = if(hasColumn(name)){
    def f(a:Any) = if(a==null)None else Some(a).as[G]
    SqlRelationColumn[Option[G]](SqlQueryPackage(sql_query_package.dataStore, sub_query(name), sql_query_package.bindVars), f _)
  }else throw ColumnDoesNotExist(name)

  override def col_asu[G](name: String)(implicit m: Manifest[G]) = if(hasColumn(name)){
    def f(a:Any) = if(a==null)None.asu[G] else Some(a).asu[G]
    SqlRelationColumn[G](SqlQueryPackage(sql_query_package.dataStore, sub_query(name), sql_query_package.bindVars), f _)
  }else throw ColumnDoesNotExist(name)
  

 
  case class SqlRelationIterator[+T] private[SqlRelation](val sql_query_package : SqlQueryPackage)
    extends SqlIterator[DataRow[T]](sql_query_package){

    lazy private val column_names = sql_query_package.generate_column_names()
    lazy private val column_count = sql_query_package.columnCount

    override def generate_next(): DataRow[T] = {
      val row = (1 to column_count) map (next_item_in_column(_))
      val dr = DataRow(column_names)(row map ((x: Any) => if (x == null) None else Some(x.asInstanceOf[T])))
      //This may not be the best spot to perform this operation, but it should suffice for the current implementation
      //TODO As the above comment suggests, this seems a bad place for this. Probably, what's needed is a custom SqlDataRow imlpementation
      //TODO refactor so that if possible, the Iterator is responsible for closing statements
      /*if(!hasNext()) {
        result_set.close()
      }*/
      dr
    }
  }

}


