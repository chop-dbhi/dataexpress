package edu.chop.cbmi.dataExpress.backends.file

import edu.chop.cbmi.dataExpress.dataModels.{ColumnNameGenerator, DataRow}

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/9/13
 * Time: 9:58 AM
 * To change this template use File | Settings | File Templates.
 */
abstract class Marshaller(cng:ColumnNameGenerator) {
  lazy val col_names = cng.generate_column_names()

  def unmarshall(line:String) : DataRow[_]

  def marshall(row: DataRow[_]) : String
}

case class CustomMarshaller(cng: ColumnNameGenerator, unmarshaller: (String)=>DataRow[_], marshaller: (DataRow[_])=> String) extends Marshaller(cng){
  def unmarshall(line : String) = unmarshaller(line)

  def marshall(row: DataRow[_]) = marshaller(row)
}

case class DelimiterMarshaller(delimiter: String, cng: ColumnNameGenerator) extends Marshaller(cng){
  def unmarshall(line:String) = {
    val items: Array[String] = line.split(delimiter).map{s=>
      val ts = s.trim
      if(ts.length==0)null
      else ts
    }
    //if the last entry is missing eg row ends with delimiter then padd with None
    val paddedItems = if(items.length==col_names.length-1)(items.toList.:+(null)).toArray else items
    val rowEntries = col_names.zip(paddedItems)
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

}

case class StaticMarshaller(cng:ColumnNameGenerator) extends Marshaller(cng) {
  def unmarshall(line:String) = {
    if(line.trim.length==0)DataRow((col_names.head,null)) else DataRow((col_names.head,line))
  }

  def marshall(row: DataRow[_]) = row.head match{
    case Some(line) => line.toString
    case _ => ""
  }


}

//case class RegexParser(cng: ColumnNameGenerator, rx:

