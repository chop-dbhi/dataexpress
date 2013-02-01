package edu.chop.cbmi.dataExpress.dataModels.sql

import edu.chop.cbmi.dataExpress.dataModels.{DataTable,DataRow}

/**
 * SqlTransform provides a formal mechanism for transforming the rows of tables.
 * SqlTransform wraps another DataTable and applies a
 * function which transforms the rows of the table. The result is a new
 * DataTable which may columns that differ in number and type from the
 * source table.
 * 
 * @param sourceTable The table where rows will be drawn
 * @param transformation function that converts DataRows from the source table into new rows
 */

case class SqlTransform[T](sourceTable:DataTable[_])(transformation:DataRow[_] => DataRow[T]) extends DataTable[T] {
	/** As seen from class SqlTransform, the missing signatures are as follows. 
	 * *  For convenience, these are usable as stub implementations.  */   
  // Members declared in edu.chop.cbmi.dataExpress.dataModels.DataTable   

  //Considering moving these out as a trait or completely eliminating
  override def col(name: String): Iterator[Option[T]] = ???
  def col_as[G](name: String)(implicit m: scala.reflect.Manifest[G]): Iterator[Option[G]] = ???
  def col_asu[G](name: String)(implicit m: scala.reflect.Manifest[G]): Iterator[G] = ???
  
  // Members declared in edu.chop.cbmi.dataExpress.dataModels.Metadata   
  lazy val columnCount: Int = columnNames.length  
  lazy val columnNames: Seq[String] = getColumns
  lazy val dataTypes: Seq[edu.chop.cbmi.dataExpress.dataModels.DataType] = ???
  
  //Not crazy about maintaining vars, but all this state needs to go somewhere
  private var cursor_advanced = false
  private var nextRow:Option[DataRow[T]] = None
  private var more_rows = true
  
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
      //Calling hasNext purely for side effects is not a not great solution to a bad situation in general
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