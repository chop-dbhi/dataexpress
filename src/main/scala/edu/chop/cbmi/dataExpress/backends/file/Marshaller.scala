package edu.chop.cbmi.dataExpress.backends.file

import edu.chop.cbmi.dataExpress.dataModels.{Metadata, DataType, DataRow}
import edu.chop.cbmi.dataExpress.dataModels.sql.{BooleanDataType, FloatDataType, IntegerDataType, TextDataType}

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/9/13
 * Time: 9:58 AM
 */
abstract class Marshaller(columns: => Seq[String]) extends Metadata  {

  override def columnNames = columns

  def unmarshall(line:String) : DataRow[_]

  def marshall(row: DataRow[_]) : String

  override def columnCount = columnNames.length

//  override lazy val dataTypes : Seq[DataType]

  def marshallHeader(row:DataRow[String]) : String  = ((""/:row){(h,cn)=> s"$h${cn.getOrElse("")},"}).dropRight(1)
}


object CustomMarshaller {
  def apply(columns: => Seq[String], unmarshaller: (String)=>DataRow[_], marshaller: (DataRow[_])=> String, dataTypeFromColName: (String)=>DataType) = {
    new CustomMarshaller(columns, unmarshaller, marshaller, dataTypeFromColName)
  }
}
class CustomMarshaller(columns: => Seq[String], unmarshaller: (String)=>DataRow[_], marshaller: (DataRow[_])=> String, dataTypeFromColName: (String)=>DataType) extends Marshaller(columns){
  def unmarshall(line : String) = unmarshaller(line)

  def marshall(row: DataRow[_]) = marshaller(row)

  override lazy val dataTypes = columnNames.map{cn => dataTypeFromColName(cn)}.toSeq

}


object DelimiterCustomMarshaller {
  def apply(delimiter: String, columns: => Seq[String],
            unmarshaller: (Array[Option[String]])=>Array[Any]) = {
    new DelimiterCustomMarshaller(delimiter, columns, unmarshaller)
  }
}
class DelimiterCustomMarshaller(delimiter: String, columns: => Seq[String],
                                unmarshaller: (Array[Option[String]])=>Array[Any])
                                extends Marshaller(columns){

  private lazy val unr = s"$delimiter".r
  private lazy val expandedDelimiter = s" $delimiter "

  override lazy val dataTypes = columnNames.toList.map{x => TextDataType}

  def unmarshall(line : String) = {
    val items: Array[Option[String]] = unr.split(unr.replaceAllIn(line,expandedDelimiter)).map{s=>
      val ts = s.trim
      if(ts.length==0)None
      else Some(ts)
    }
    val rowEntries = columnNames.zip(unmarshaller(items))
    DataRow(rowEntries: _*)
  }

  def marshall(row: DataRow[_]) = {
    (""/:row){(s,o) =>
      o match{
        case Some(v) => s"$s${v.toString}$delimiter"
        case _ => s"$s$delimiter"
      }
    }.dropRight(1)
  }

  override def marshallHeader(row:DataRow[String]) : String  = ((""/:row){(h,cn)=> s"$h${cn.getOrElse("")}$delimiter"}).dropRight(1)

}

object DelimiterMarshaller {
  def apply(delimiter: String, columns: => Seq[String]) = {
    new DelimiterMarshaller(delimiter, columns)
  }
}

class DelimiterMarshaller(delimiter: String, columns: => Seq[String]) extends Marshaller(columns){

  private lazy val unr = s"$delimiter".r
  private lazy val expandedDelimiter = s" $delimiter "

  val dataTypes = columnNames.toList.map{x => TextDataType}

  def unmarshall(line:String) = {
    val items: Array[String] = unr.split(unr.replaceAllIn(line,expandedDelimiter)).map{s=>
      val ts = s.trim
      if(ts.length==0)null
      else ts
    }
    val rowEntries = columnNames.zip(items)
    DataRow(rowEntries: _*)
  }

  def marshall(row: DataRow[_]) = {
    (""/:row){(s,o) =>
      o match{
        case Some(v) => s"$s${v.toString}$delimiter"
        case _ => s"$s$delimiter"
      }
    }.dropRight(1)
  }

  override def marshallHeader(row:DataRow[String]) : String  = ((""/:row){(h,cn)=> s"$h${cn.getOrElse("")}$delimiter"}).dropRight(1)

}

abstract class TypedDelimiterMarshaller[TYPE](delimiter: String, columns: =>  Seq[String], f: (String)=> TYPE) extends Marshaller(columns){
  private lazy val unr = s"$delimiter".r
  private lazy val expandedDelimiter = s" $delimiter "

  def unmarshall(line:String) = {
    val items: List[Option[TYPE]] = unr.split(unr.replaceAllIn(line,expandedDelimiter)).map{s=>
      val ts = s.trim
      if(ts.length==0)None
      else Some(f(ts))
    }.toList
    val rowEntries = columnNames.zip(items.map{item=>
      item match{
        case Some(i) => i
        case _ => null
      }
    })
    DataRow(rowEntries: _*)
  }

  def marshall(row: DataRow[_]) = {
    (""/:row){(s,o) =>
      o match{
        case Some(v) => s"$s${v.toString}$delimiter"
        case _ => s"$s$delimiter"
      }
    }.dropRight(1)
  }

  override def marshallHeader(row:DataRow[String]) : String  = ((""/:row){(h,cn)=> s"$h${cn.getOrElse("")}$delimiter"}).dropRight(1)

}

object IntDelimiterMarshaller {
  def apply(delimiter: String, columns: => Seq[String]) = {
    new IntDelimiterMarshaller(delimiter, columns)
  }
}

class IntDelimiterMarshaller(delimiter: String, columns: => Seq[String])
  extends TypedDelimiterMarshaller[Int](delimiter, columns, (s:String)=> s.toInt){

  def dataTypes = col_names.toList.map{x => IntegerDataType}
}

object DoubleDelimiterMarshaller {
  def apply(delimiter: String, columns: => Seq[String], precision:Int = 10) = {
    new DoubleDelimiterMarshaller(delimiter, columns, precision)
  }
}

class DoubleDelimiterMarshaller(delimiter: String, columns: => Seq[String], precision:Int = 10)
  extends TypedDelimiterMarshaller[Double](delimiter, columns, (s:String)=> s.toDouble){

  val dataTypes = columnNames.toList.map{x => FloatDataType(precision)}
}


object BooleanDelimiterMarshaller {
  def apply(delimiter: String, columns: => Seq[String]) = {
    new BooleanDelimiterMarshaller(delimiter, columns)
  }
}
class BooleanDelimiterMarshaller(delimiter: => String, columns: => Seq[String])
  extends TypedDelimiterMarshaller[Boolean](delimiter, columns, (s:String)=> s.toBoolean){

  val dataTypes = columnNames.toList.map{x => BooleanDataType}
}

object StaticMarshaller  {
  def apply(columns: => Seq[String]) = {
    new StaticMarshaller(columns)
  }
}

class StaticMarshaller(columns: => Seq[String]) extends Marshaller(columns){
  val dataTypes = columnNames.toList.map{x => TextDataType}

  def unmarshall(line:String) = {
    if(line.trim.length==0)DataRow((col_names.head,null)) else DataRow((col_names.head,line))
  }

  def marshall(row: DataRow[_]) = row.head match{
    case Some(line) => line.toString
    case _ => ""
  }


}
