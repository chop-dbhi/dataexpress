package edu.chop.cbmi.dataExpress.backends.file

import java.io.{File, FileWriter, BufferedWriter, PrintWriter}
import edu.chop.cbmi.dataExpress.dataModels.{DataRow, Metadata}
import scala.io.Source

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/8/13
 * Time: 1:21 PM
 */

object TextFileBackend{
  def apply(file:File, columns: => Option[Seq[String]], readSkipLines : Int, options: MagicOptions, encoding: String) : TextFileBackend =
    new TextFileBackend(file, MagicMarshaller(file, columns, options), readSkipLines, encoding )

  def apply(file: File, columns: => Option[Seq[String]] = None) : TextFileBackend = columns match{
    case None => TextFileBackend(file, MagicMarshaller(file, None, new MagicOptions), 1, "UTF-8")
    case _ => TextFileBackend(file, MagicMarshaller(file, columns, new MagicOptions), 0, "UTF-8")
  }

  def apply(file:File) : TextFileBackend= new TextFileBackend(file, MagicMarshaller(file), 1, "UTF-8")

  def apply(file: File, marshaller: Marshaller, readSkipLines: Int, encoding: String ) : TextFileBackend =
    new TextFileBackend(file,marshaller, readSkipLines, encoding)

  def apply(file: File, marshaller: Marshaller, readSkipLines: Int): TextFileBackend = new TextFileBackend(file, marshaller, readSkipLines, "UTF-8")

  def apply(file: File, marshaller: Marshaller) : TextFileBackend= new TextFileBackend(file, marshaller, 0, "UTF-8")

  def getHeaderRowColumnNames(file: File, delimiter: String = ",", encoding: String="UTF-8") = {
    val unr = s"$delimiter".r
    val expandedDelimiter = s" $delimiter "
    val source = Source.fromFile(file, encoding)
    val iter = source.getLines()
    val names = if(iter.isEmpty)Seq.empty[String] else{
        unr.split(unr.replaceAllIn(iter.next, expandedDelimiter)).map{s => s.trim}.toSeq
      }
      source.close()
      names
    }
}

class TextFileBackend(override val file: File, marshaller: Marshaller, readSkipLines : Int, encoding: String) extends FileBackend(file){

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

    source.close()

