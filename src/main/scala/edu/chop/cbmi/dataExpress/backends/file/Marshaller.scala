package edu.chop.cbmi.dataExpress.backends.file

import edu.chop.cbmi.dataExpress.dataModels.{DataType,ColumnNameGenerator, DataRow}
import edu.chop.cbmi.dataExpress.dataModels.sql.{BooleanDataType, FloatDataType, IntegerDataType, TextDataType}

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/9/13
 * Time: 9:58 AM
 */
abstract class Marshaller(cng:ColumnNameGenerator) {
  lazy val col_names = cng.generate_column_names()

  def unmarshall(line:String) : DataRow[_]

  def marshall(row: DataRow[_]) : String

  def dataTypes() : Seq[DataType]

  def marshallHeader(row:DataRow[String]) : String  = ((""/:row){(h,cn)=> s"$h${cn.getOrElse("")},"}).dropRight(1)
}

class CustomMarshaller(cng: ColumnNameGenerator, unmarshaller: (String)=>DataRow[_], marshaller: (DataRow[_])=> String, dataTypeFromColName: (String)=>DataType) extends Marshaller(cng){
  def unmarshall(line : String) = unmarshaller(line)

  def marshall(row: DataRow[_]) = marshaller(row)

  lazy val dataTypes = col_names.map{cn => dataTypeFromColName(cn)}.toSeq

}


case class DelimiterCustomMarshaller(delimiter: String, cng: ColumnNameGenerator,
                                unmarshaller: (Array[Option[String]])=>Array[Any])
                                extends Marshaller(cng){

  private lazy val unr = s"$delimiter".r
  private lazy val expandedDelimiter = s" $delimiter "

  def dataTypes() = col_names.toList.map{x => TextDataType}

  def unmarshall(line : String) = {
    val items: Array[Option[String]] = unr.split(unr.replaceAllIn(line,expandedDelimiter)).map{s=>
      val ts = s.trim
      if(ts.length==0)None
      else Some(ts)
    }
    val rowEntries = col_names.zip(unmarshaller(items))
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

case class DelimiterMarshaller(delimiter: String, cng: ColumnNameGenerator) extends Marshaller(cng){

  private lazy val unr = s"$delimiter".r
  private lazy val expandedDelimiter = s" $delimiter "

  def dataTypes() = col_names.toList.map{x => TextDataType}

  def unmarshall(line:String) = {
    val items: Array[String] = unr.split(unr.replaceAllIn(line,expandedDelimiter)).map{s=>
      val ts = s.trim
      if(ts.length==0)null
      else ts
    }
    val rowEntries = col_names.zip(items)
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

abstract class TypedDelimiterMarshaller[TYPE](delimiter: String, cng: ColumnNameGenerator, f: (String)=> TYPE) extends Marshaller(cng){
  private lazy val unr = s"$delimiter".r
  private lazy val expandedDelimiter = s" $delimiter "

  def unmarshall(line:String) = {
    val items: List[Option[TYPE]] = unr.split(unr.replaceAllIn(line,expandedDelimiter)).map{s=>
      val ts = s.trim
      if(ts.length==0)None
      else Some(f(ts))
    }.toList
    val rowEntries = col_names.zip(items.map{item=>
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

case class IntDelimiterMarshaller(delimiter: String, cng: ColumnNameGenerator)
  extends TypedDelimiterMarshaller[Int](delimiter, cng, (s:String)=> s.toInt){

  def dataTypes = col_names.toList.map{x => IntegerDataType}
}

case class DoubleDelimiterMarshaller(delimiter: String, cng: ColumnNameGenerator, precision:Int = 10)
  extends TypedDelimiterMarshaller[Double](delimiter, cng, (s:String)=> s.toDouble){

  def dataTypes = col_names.toList.map{x => FloatDataType(precision)}
}

case class BooleanDelimiterMarshaller(delimiter: String, cng: ColumnNameGenerator)
  extends TypedDelimiterMarshaller[Boolean](delimiter, cng, (s:String)=> s.toBoolean){

  def dataTypes = col_names.toList.map{x => BooleanDataType}
}

case class StaticMarshaller(cng:ColumnNameGenerator) extends Marshaller(cng){
  def dataTypes() = col_names.toList.map{x => TextDataType}

  def unmarshall(line:String) = {
    if(line.trim.length==0)DataRow((col_names.head,null)) else DataRow((col_names.head,line))
  }

  def marshall(row: DataRow[_]) = row.head match{
    case Some(line) => line.toString
    case _ => ""
  }


}
