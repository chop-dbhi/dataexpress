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
package edu.chop.cbmi.dataExpress.dsl

import edu.chop.cbmi.dataExpress.dataModels.{DataRow, DataType, DataTable}


/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 12/30/11
 * Time: 8:44 AM
 * To change this template use File | Settings | File Templates.
 */

//abstract class TransformableDataTable() {
//
//  val _transformers : scala.collection.mutable.ListBuffer[(DataRow[_]) => Option[DataRow[_]]]
//  var _final_column_names : List[String]
//  var _final_data_types: Option[List[DataType]]
//
//  def data_table() : DataTable[_]
//
//  def transform_rows(f: (DataRow[_]) => DataRow[_]): TransformableDataTable = {
//    def wrapped_transform(transform: (DataRow[_]) => DataRow[_])(row: DataRow[_]) = Some(f(row))
//    _transformers += wrapped_transform(f) _
//    this
//  }
//
//  def filter_rows(f: (DataRow[_]) => Boolean): TransformableDataTable = {
//    def wrapped_filter(filtration: (DataRow[_]) => Boolean)(row: DataRow[_]) = if (filtration(row)) Some(row) else None
//    _transformers += wrapped_filter(f) _
//    this
//  }
//
//}
//
//class FixedDimensionTransformableTable(private val table: DataTable[_]) extends TransformableDataTable() {
//
//  val _transformers = scala.collection.mutable.ListBuffer.empty[(DataRow[_]) => Option[DataRow[_]]]
//  var _final_column_names: List[String] = table.columnNames.toList
//  var _final_data_types: Option[List[DataType]] = None
//
//  def data_table() = table
//
//  private def update_column_names(names: String*): FixedDimensionTransformableTable = {
//    _final_column_names = names.toList
//    transform_rows((dr: DataRow[_]) => {
//      val items = dr.columnNames.zipWithIndex.sortWith((t1: (String, Int), t2: (String, Int)) => {
//        t1._2 < t2._2
//      }).map((t: (String, Int)) => {
//        names(t._2) -> dr(t._1).getOrElse(null)
//      })
//      DataRow(items: _*)
//    })
//    this
//  }
//
//  def change_column_names(old_to_new: (String, String)*): FixedDimensionTransformableTable = {
//    val old_names = old_to_new map {
//      _._1
//    }
//    val name_map = old_to_new.toMap
//
//    val temp = _final_column_names map {
//      name =>
//        if (old_names.contains(name)) name_map(name) else name
//    }
//    update_column_names(temp: _*)
//  }
//
//  def alter() = new AlterDataTable(this)
//
//}
//
//case class AlterDataTable(table: TransformableDataTable) extends TransformableDataTable(){
//  val _transformers = table._transformers
//  var _final_column_names: List[String] = table._final_column_names
//  var _final_data_types: Option[List[DataType]] = table._final_data_types
//
//  def data_table() = table.data_table()
//
//  def with_column_names(names: String*): AlterDataTable = {
//    _final_column_names = names.toList
//    this
//  }
//
//  def with_data_types(data_type: DataType*): AlterDataTable = {
//    _final_data_types = Some(data_type.toList)
//    this
//  }
//}