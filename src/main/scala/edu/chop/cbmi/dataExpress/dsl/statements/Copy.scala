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
package edu.chop.cbmi.dataExpress.dsl.statements

import edu.chop.cbmi.dataExpress.dsl.stores.{Store}
import edu.chop.cbmi.dataExpress.dataModels.{DataType, DataRow}
import edu.chop.cbmi.dataExpress.dsl._

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 1/12/12
 * Time: 2:03 PM
 * To change this template use File | Settings | File Templates.
 */

trait CopyPre{
  def from(source : Store) : CopyFrom
}

trait Transformer[T<:Transformer[T]] {

  self: T =>

  def table() : TransformableDataTable

  def filter(f:(DataRow[_])=>Boolean) : T = {
    table.filter_rows(f)
    this
  }

  def transform(f : (DataRow[_])=>DataRow[_]) : T = {
    table.transform_rows(f)
    this
  }

  def to(target : Store) : ToFromTable = ToFromTable(target, table)
}

trait CopyFromGeneric extends Transformer[CopyFromGeneric]

abstract class CopyFrom extends CopyFromGeneric {
  protected val _table : FixedDimensionTransformableTable

  def table() = _table

  def change_column_names(old_to_new: (String, String)*): CopyFrom = {
    table.change_column_names(old_to_new: _*)
    this
  }

  def alter(code : (CopyFromAlter)=>CopyFromAlterFinal) : Transformer[CopyFromGeneric] = {
    code(new CopyFromAlter(table))
  }
}

class CopyFromAlterFinal(val transform_table : TransformableDataTable) extends CopyFromGeneric{
  def table() = transform_table
}

class CopyFromAlter(transform_table : TransformableDataTable) extends CopyFromGeneric{

  private val _table = new AlterDataTable(transform_table)

  def table() = _table

  def set_column_names(names: String*): CopyFromAlter = {
    table.with_column_names(names: _*)
    this
  }

  def set_data_types(data_type: DataType*): CopyFromAlter = {
    table.with_data_types(data_type: _*)
    this
  }

  def set_row_values(f: (DataRow[_]) => DataRow[_]): CopyFromAlterFinal = {
    table.transform_rows(f)
    new CopyFromAlterFinal(table)
  }

}

class CopyFromTablePre(table_name : String) extends CopyPre {
  def from(source : Store) = new CopyFromTable(table_name, source)
}

class CopyFromTable(table_name : String, source : Store) extends CopyFrom{
  lazy protected val _table = From(source).get_table(table_name)
}

class CopyFromQueryPre(query : String) extends CopyPre{
  private var _bind_vars : Seq[Option[Any]] = Seq.empty[Option[Any]]

  def from(source : Store) = new CopyFromQuery(query, source, _bind_vars)

  def using_bind_vars(bind_var : Any*) = {
    _bind_vars = bind_var.toSeq map {Some(_)}
    this
  }
}

class CopyFromQuery(query : String, source : Store, bind_vars : Seq[Option[Any]]) extends CopyFrom{
  lazy protected val _table = From(source).query(query, bind_vars)
}

class CopyFromSource(source: Store) extends CopyFrom{
  lazy protected val _table = From(source).get_values
}

class CopySelect {
  def table(table_name : String) : CopyFromTablePre = new CopyFromTablePre(table_name)

  def query(q : String) : CopyFromQueryPre = new CopyFromQueryPre(q)

  def from(source: Store) : CopyFromSource = new CopyFromSource(source)
}

