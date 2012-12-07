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
 * Date: 11/11/11
 * Time: 12:29 PM
 * To change this template use File | Settings | File Templates.
 */

import scala.reflect.Manifest
import edu.chop.cbmi.dataExpress.dataModels.RichOption._
import edu.chop.cbmi.dataExpress.exceptions.ColumnDoesNotExist
import DataRow.map_to_option
import edu.chop.cbmi.dataExpress.exceptions.ColumnDoesNotExist

/**
 * A simple immutable [[edu.chop.cbmi.dataExpress.dataModels.DataTable]] that maintains all data elements in memory
 */
case class SimpleDataTable[+T] private[dataModels](override val column_names_generator: ColumnNameGenerator)(private val data: Seq[Seq[T]])
  extends DataTable[T](column_names_generator) with Iterator[DataRow[T]] {
  private var index = 0
  require(data.length > 0, println("data cannot be empty"))
  require(column_names.length == data(0).length, println("generate_column_names.length must equal data(0).length"))
  require((true /: data)((b: Boolean, l: Seq[_]) => b && l.length == data(0).length),
    println("All elements in data must be of equal length"))
  private val  iterator = SimpleDataIterator(column_names, data)
  
   /**
   * @param idx index of desired table row
   * @return DataRow[Option[T]] containing the elements of row idx wrapped in an Option
   */
   def apply(idx: Int) = DataRow(column_names)(map_to_option(data(idx)))

  /**
   * @return int the number of rows
   */
  override def length = data.length

  override def hasNext = iterator.hasNext
  
  override def next = iterator.next()

  override def col(name: String) : Iterator[Option[T]] = if(hasColumn(name)) {
    val idx = column_names.indexOf(name)
    data.iterator.map((l: Seq[T]) => if (l(idx) == null) None else Some(l(idx)))
  } else throw ColumnDoesNotExist(name)

  override def col_as[G](name: String)(implicit m: Manifest[G]) : Iterator[Option[G]] = if(hasColumn(name)) {
    val idx = column_names.indexOf(name)
    data.iterator.map((l: Seq[T]) => if (l(idx) == null) None else Some(l(idx)).as[G])
  } else throw ColumnDoesNotExist(name)

  override def col_asu[G](name: String)(implicit m: Manifest[G]) : Iterator[G] = if(hasColumn(name)) {
    val idx = column_names.indexOf(name)
    data.iterator.map((l: Seq[T]) => if (l(idx) == null) None.asu[G] else Some(l(idx)).asu[G])
  } else throw ColumnDoesNotExist(name)

  override def applyDynamic(name: String)(args: Any*): Iterator[Option[T]] = {
    if (hasColumn(name)) this.col(name)
    else throw ColumnDoesNotExist(name)
  }

  case class SimpleDataIterator[+T] private[SimpleDataTable]
  (private val column_names: Seq[String], private val data: Seq[Seq[T]]) extends Iterator[DataRow[T]] {

    private var index = 0

    def hasNext() = index < data.length

    def next(): DataRow[T] = {
      index += 1
      DataRow(column_names)(map_to_option(data(index-1)))
    }
  }

}