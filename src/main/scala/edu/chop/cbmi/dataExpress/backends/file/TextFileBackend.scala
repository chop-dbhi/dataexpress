package edu.chop.cbmi.dataExpress.backends.file

import java.io.{File, FileWriter, BufferedWriter, PrintWriter}
import edu.chop.cbmi.dataExpress.dataModels.{ColumnNameGenerator, DataRow}
import scala.io.Source

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/8/13
 * Time: 1:21 PM
 */
case class TextFileBackend(override val file: File, marshaller: Marshaller, readSkipLines : Int = 0, encoding: String="UTF-8") extends FileBackend(file){

  def dataTypes() = marshaller.dataTypes()

  def writeHeader(hr: DataRow[String]) = {
    printToFile{pw =>
      pw.println(marshaller.marshallHeader(hr))
    }
  }

  def read() : Iterator[DataRow[_]] ={
    TextIterator(open.getLines().drop(readSkipLines), marshaller)
  }

  def write(rows: Iterator[DataRow[_]], writeMode: WriteMode) : Boolean = {
    close
    writeMode match{
      case Overwrite => {
        printToFile{ pw =>
          rows.foreach{ row => pw.println(marshaller.marshall(row)) }
        }
        true
      }
      case Append => {
        appendToFile{pw =>
         rows.foreach{ row => pw.println(marshaller.marshall(row))}
        }
        true
      }

      case _ => throw new WriteModeException(writeMode, "Unsupported WriteMode")
    }
  }

  def write(row: DataRow[_], writeMode: WriteMode) = write(List(row).iterator, writeMode)


  private def printToFile(op: PrintWriter => Unit) {
    makeNewFile()
    val pw = new PrintWriter(file)
    try {
      op(pw)
    } finally {
      pw.close()
    }
  }

  private def appendToFile(op: PrintWriter => Unit) {
    if(!file.exists())makeNewFile()
    val pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))
    try { {
      op(pw)
    }
    } finally {
      pw.close()
    }
  }

}

case class TextIterator(lineIterator: Iterator[String], parser : Marshaller) extends Iterator[DataRow[_]]{
 override def hasNext() = lineIterator.hasNext

 override def next() = parser.unmarshall(lineIterator.next)
}

case class HeaderRowColumnNames(file: File, delimiter: String = ",", encoding: String="UTF-8") extends ColumnNameGenerator{

  private def columnNames = {
    val source = Source.fromFile(file, encoding)
    val iter = source.getLines()
    val names = if(iter.isEmpty)Seq.empty[String]
    else{
      val header = iter.next()
      header.split(delimiter).toSeq

    }
    source.close()
    names
  }


  def generate_column_names() = columnNames
}
