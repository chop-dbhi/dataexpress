package edu.chop.cbmi.dataExpress.dataModels

import reflect.Manifest


/**
 * Provides a formal mechanism for transforming the rows of tables.
 * Wraps another DataTable and applies a
 * function which transforms the rows of the table. The result is a new
 * DataTable which may have columns that differ in number and type from the
 * source table.
 * 
 * @param sourceTable The table where rows will be drawn
 * @param transformation function that converts DataRows from the source table into new rows
 */

object TransformedDataTable {
  def apply[T](sourceTable:DataTable[T])(transformation:DataRow[T] => DataRow[T]) = {
    new TransformedDataTable[T](sourceTable)(transformation)
  }

  def apply[T]()
}


class TransformedDataTable[T](sourceTable:DataTable[T])(transformation:DataRow[T] => DataRow[T])extends DataTable[T] {

  //Considering moving these out as a trait or completely eliminating

  // Members declared in edu.chop.cbmi.dataExpress.dataModels.Metadata   
  lazy val columnCount: Int = columnNames.length
  //TODO: data tables aren't required to have columnNames, only ones that implement metadata
  //Need to figure out a generic way to handle this properly
  lazy val columnNames: Seq[String] = getColumns
  lazy val dataTypes: Seq[edu.chop.cbmi.dataExpress.dataModels.DataType] = ???
  
  //Not crazy about maintaining vars, but all this state needs to go somewhere
  private var cursor_advanced = false
  private var nextRow:Option[DataRow[T]] = None
  private var more_rows = true

  override def col(name: String): Iterator[Option[T]] = {
    val singleCol: TransformedDataTable[T] = new TransformedDataTable[T](this)({
      dr:DataRow[T] => DataRow(name -> dr(name).get)
    })
    singleCol.map{r => r.selectDynamic(name)}
  }

  override def col_as[G](name: String)(implicit m: Manifest[G]): Iterator[G] = this.col_asu(name)

  override def col_asu[G](name: String)(implicit m: Manifest[G]): Iterator[G] = {
    this.col(name).map{r => r.get.asInstanceOf[G]}
  }


  override def hasNext() = {
    if (!cursor_advanced) {
      cursor_advanced = true
      nextRow = Some(transform())
      more_rows = nextRow match {
        case None => false
        case _ => true 
      }
    }
    more_rows
  }

  override def next() = {
    
    if (cursor_advanced) {
      cursor_advanced = false
    }
    
    else { 
      val row = transform()
      nextRow = Some(row)
    }
    nextRow.get
  }
   
  private def getColumns:Seq[String] = {
    nextRow match {
      case Some(row) => row.columnNames
      //Calling hasNext purely for side effects is a "not great" solution to a bad situation in general
      case None => if(hasNext()) { 
    	  nextRow match {
    	    case Some(row) => row.columnNames
    	    case None => Seq[String]()
    	  }
      }
      else { Seq[String]()}
    }
  }
  
  private def transform() = {
    val row = transformation(sourceTable.next())
    row
  }


}