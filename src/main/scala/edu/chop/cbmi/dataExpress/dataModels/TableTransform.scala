package edu.chop.cbmi.dataExpress.dataModels


object TableTransform {
  def transform(table:DataTable[T])(f:DataRow[_] => DataRow[_]): TransformedDataTable[T] = {
    TransformedDataTable(table)(f)
  }

  def filterRows(table:DataTable[T])(f:DataRow[_] => Boolean): TransformedDataTable[T] = {
   TransformedDataTable(table)()

  }

  def changeColumnNames(table:DataTable[T])(names: Seq[String]): TransformedDataTable[T] = {

  }

}
