/*
Copyright (c) 2012, The Children's Hospital of Philadelphia All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
   disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
   following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.chop.cbmi.dataExpress.dataModels

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 11/2/11
 * Time: 2:22 PM
 * To change this template use File | Settings | File Templates.
 */

import scala.reflect.Manifest
import edu.chop.cbmi.dataExpress.backends.SqlBackend

import edu.chop.cbmi.dataExpress.dataModels.sql.SqlRelation
import edu.chop.cbmi.dataExpress.dataModels.sql.SqlQueryPackage
import edu.chop.cbmi.dataExpress.exceptions.ColumnDoesNotExist

trait ColumnNameGenerator{
  /**
   * method to generate column_names.
   */
  def generate_column_names() : Seq[String]
}

case class SeqColumnNames(column_names : Seq[String]) extends ColumnNameGenerator {
  def generate_column_names() = column_names
}

/**
 * A Wrapper class that extends Seq[T] with Dynamic to enable row element access using the column names with dot notation
 * e.g. Given a DataRow instance, dr, with column_names = Seq("a","b") and data = Seq(1,2), the values
 * can be accessed as dr.a and dr.b
 */
case class DataRow[+T] (column_names : Seq[String])(private val data : Seq[Option[T]]) extends Seq[Option[T]] with Dynamic{
  require(column_names.length == data.length)
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
  def applyDynamic(name : String)(args: Any*) = find_value(name)

  protected def find_value(name : String) = {
    if(column_names.contains(name))data(column_names.indexOf(name))
    else throw ColumnDoesNotExist(name)
  }
}

/** Factory for [[edu.chop.cbmi.dataExpress.dataModels.DataRow]]*/
object DataRow{

  def apply[T](values : (String,T)*) : DataRow[T] = {
    val col_names = values map((t:(String,T))=>t._1)
    val items = values map((t:(String, T))=>t._2)
    DataRow(col_names)(map_to_option(items))
  }

  def map_to_option[T](l: Seq[T]) = l map((t:T)=> if(t==null) None else Some(t))

  def empty = DataRow(Seq.empty[String])(Seq.empty[Option[Nothing]])

}

/**
 * base class for other data representation classes organized as a 2-D table with column names.
 */
//TODO: Issue 14 should extend Iterator instead of Iterable 
abstract case class DataTable[+T](val column_names_generator: ColumnNameGenerator) extends Iterator[DataRow[T]] with Dynamic{

  lazy val column_names = column_names_generator.generate_column_names()

  /**
   * @param name name of column
   * @return boolean corresponding to existence of the column in this table
   */
  def hasColumn(name: String) = column_names.contains(name)

  /**
   * @param name name of column
   * @return an iterable collection of Option[T] of the elements in the column wrapped in an Option
   */
  def col(name: String) : Iterator[Option[T]]

  /**
   * @param G the desired return type
   * @param name the name of the column
   * @return an iterable collection of Option[G] of the elements in the column cast to type G wrapped in an Option
   */
  def col_as[G](name: String)(implicit m: Manifest[G]): Iterator[Option[G]]

  /**
   * @param G the desired return type
   * @param name the name of the column
   * @return an iterable collection of Option[G] of the elements in the column cast to type G
   * Note this method will likely fail if null instances exist in the column
   */
  def col_asu[G](name: String)(implicit m: Manifest[G]): Iterator[G]

  def applyDynamic(name : String)(args: Any*) = {
    if(hasColumn(name))this.col(name)
    else throw ColumnDoesNotExist(name)
  }


  override def toString() : String = "DataTable[" + (column_names.head /: column_names.tail) { (s1,s2) => s1 + ", " + s2} + "]"
}

/** Factory for [[edu.chop.cbmi.dataExpress.dataModels.DataTable]] */
object DataTable {

  private def column_name_from_index(i: Int) = {
    val alphabet = "abcdefghijklmnopqrstuvwxyz"
    "a" * (i / 26) + alphabet(i % 26)
  }

  def empty = apply(Seq.empty[String], Seq.empty[Nothing])

  def apply[T](column_names: Seq[String], row: Seq[T]*): SimpleDataTable[T] = {
    val rows = List(row: _*)
    SimpleDataTable(SeqColumnNames(column_names))(rows)
  }

  def apply[T](row: Seq[T]*): SimpleDataTable[T] = {
    val cns = (0 to (row.length - 1)) map ((i: Int) => column_name_from_index(i))
    apply(cns, row: _*)
  }

  def apply(dataStore : SqlBackend, query : String, bindVars : Seq[Option[_]] = Seq.empty[Option[_]]) = {
    SqlRelation(SqlQueryPackage(dataStore,query,bindVars))
  }

}