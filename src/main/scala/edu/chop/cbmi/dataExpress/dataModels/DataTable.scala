package edu.chop.cbmi.dataExpress.dataModels

import scala.reflect.Manifest
import edu.chop.cbmi.dataExpress.backends.SqlBackend

import edu.chop.cbmi.dataExpress.dataModels.sql.SqlRelation
import edu.chop.cbmi.dataExpress.exceptions.ColumnDoesNotExist
import scala.language.dynamics

/**
 * Ensures a common set of Metadata traits across any classes that implement this trait. Specific implementations are
 * left to the implementing class (e.g. [[edu.chop.cbmi.dataExpress.dataModels.sql.SqlRelation]] will need to lazily
 * produce some of this at query time whereas simple tab delimited files might need to autogenerate it
 * 
 */
trait Metadata {
	val columnNames:Seq[String]
	val dataTypes:Seq[DataType]
	val columnCount:Int
}

/**
 * A Wrapper class that extends Seq[T] with [[scala.Dynamic]] to enable row element access using the column names with dot notation
 * e.g. Given a DataRow instance, dr, with columnNames = Seq("a","b") and data = Seq(1,2), the values
 * can be accessed as dr.a and dr.b
 */
case class DataRow[+T] (columnNames : Seq[String])(private val data : Seq[Option[T]]) extends Seq[Option[T]] with Dynamic{
  require(columnNames.length == data.length)
  /**
   * enables access to data elements using Map like syntax
   */
  def apply(name : String) = find_value(name)

  /**
   * enables indexed access
   */
  override def apply(idx : Int) = data(idx)

  override def iterator() = data.iterator

  override def length = data.length

  /**
   * enables access to data elements using dot notation
   */
  def selectDynamic(name : String) = find_value(name)

  protected def find_value(name : String) = {
    if(columnNames.contains(name))data(columnNames.indexOf(name))
    else throw ColumnDoesNotExist(name)
  }
}

/** Factory for [[edu.chop.cbmi.dataExpress.dataModels.DataRow]]*/
object DataRow{

  def apply[T](values : (String,T)*) : DataRow[T] = {
    val columnNames = values map((t:(String,T))=>t._1)
    val items = values map((t:(String, T))=>t._2)
    DataRow(columnNames)(map_to_option(items))
  }

  def map_to_option[T](l: Seq[T]) = l map((t:T)=> if(t==null) None else Some(t))

  def empty = DataRow(Seq.empty[String])(Seq.empty[Option[Nothing]])

}

/**
 * base class for other data representation classes organized as a 2-D table with column names.
 */
abstract class DataTable[+T] extends Iterator[DataRow[T]] with Dynamic with Metadata {


  /**
   * @param name name of column
   * @return boolean corresponding to existence of the column in this table
   */
  def hasColumn(name: String) = columnNames.contains(name)

  /**
   * @param name name of column
   * @return an iterable collection of Option[T] of the elements in the column wrapped in an Option
   */
  //def col(name: String) : Iterator[Option[T]]

  /**
   * @param G the desired return type
   * @param name the name of the column
   * @return an iterable collection of Option[G] of the elements in the column cast to type G wrapped in an Option
   */
  //def col_as[G](name: String)(implicit m: Manifest[G]): Iterator[Option[G]]

  /**
   * @param G the desired return type
   * @param name the name of the column
   * @return an iterable collection of Option[G] of the elements in the column cast to type G
   * Note this method will likely fail if null instances exist in the column
   */
  //def col_asu[G](name: String)(implicit m: Manifest[G]): Iterator[G]

  def selectDynamic(name: String) = {
    if(hasColumn(name)) {this.col(name)}
    else throw ColumnDoesNotExist(name)
  }


  override def toString() : String = "DataTable[" + (columnNames.head /: columnNames.tail) { (s1,s2) => s1 + ", " + s2} + "]"
}

/** Factory for [[edu.chop.cbmi.dataExpress.dataModels.DataTable]] */
object DataTable {
  /* 
   * Generates a column name from a numerical index using a similar algorithm to what Excel does
   */
  private def column_name_from_index(i: Int) = {
    val alphabet = "abcdefghijklmnopqrstuvwxyz"
    "a" * (i / 26) + alphabet(i % 26)
  }
  
  /** Convenience method for creating empty tables */
  def empty = apply(Seq.empty[String], Seq.empty[Nothing])
  
  /**
   * Creates a [[edu.chop.cbmi.dataExpress.dataModels.SimpleDataTable]] using an in-memory data structure
   * 
   * @param columnNames The names of the columns
   * @param row A set of {{{Seq}}} objects, each representing a row
   * 
   * @return A simple data table that holds all data in memory
   */
  def apply[T](columnNames: Seq[String], row: Seq[T]*): SimpleDataTable[T] = {
    val rows = List(row: _*)
    SimpleDataTable(columnNames)(rows)
  }
  
  /**
   * Creates a [[edu.chop.cbmi.dataExpress.dataModels.SimpleDataTable]] using an in-memory data structure with auto-generated column names
   * 
   * @param row A set of {{{Seq}}} objects, each representing a row
   */
    def apply[T](row: Seq[T]*): SimpleDataTable[T] = {
    val cns = (0 to (row.length - 1)) map ((i: Int) => column_name_from_index(i))
    apply(cns, row: _*)
  }

  /**
   * Creates a [[edu.chop.cbmi.dataExpress.dataModels.sql.SqlRelation]]
   * 
   * @param dataStore An open [[edu.chop.cbmi.dataExpress.backend.SqlBackend]] ready to accept queries
   * @param query A SQL query that returns results
   * @param bindVars (optional) set of values to bind to placeholder variables 
   */
  //TODO Should this really be here in a generic package? Seems like you would want SQLRelation to emit a data table.
  def apply(dataStore:SqlBackend, query:String, bindVars:Seq[Option[_]] = Seq.empty[Option[_]]) = {
    SqlRelation(query, bindVars, dataStore)
  }
  
  //TODO create a simple apply() method that calls .empty so this behaves like List()

}