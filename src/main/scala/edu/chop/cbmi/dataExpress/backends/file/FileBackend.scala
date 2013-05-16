package edu.chop.cbmi.dataExpress.backends.file

import java.io.{FileWriter, BufferedWriter, PrintWriter, File}
import scala.io.{BufferedSource, Source}
import edu.chop.cbmi.dataExpress.dataModels.{ColumnNameGenerator, DataType, DataRow}

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/8/13
 * Time: 12:08 PM
 */

trait WriteMode
object Overwrite extends WriteMode
object Append extends WriteMode
class WriteModeException(wm: WriteMode, msg: String) extends Exception(msg)

abstract class FileBackend(val file: File) {
  //ABSTRACT STUFF

  def writeHeader(hr: DataRow[String]) : Unit

  def dataTypes() : Seq[DataType]

  def read() : Iterator[DataRow[_]]

  def write(rows: Iterator[DataRow[_]], writeMode: WriteMode) : Boolean

  def write(row: DataRow[_], writeMode: WriteMode) : Boolean

  //CONCRETE STUFF
  private var source: Option[BufferedSource] = None
  def open() = {
    close
    val ns = Source.fromFile(file)
    source = Some(ns)
    ns
  }

  def delete() = {
    close
    if(file.exists())file.delete
  }

  def ensureParentDirs() = if(!file.getParentFile.exists())file.getParentFile.mkdirs()

  def makeNewFile() = {
    delete
    ensureParentDirs()
    file.createNewFile()
  }

  def close() = {
    source match{
      case Some(s) => s.close
      case _=> {}
    }
    source = None
    true
  }

  def is_open_? = source match{
    case Some(s) => {
      if(s.isEmpty){
        close
        false
      }else true
    }
    case _ => false
  }

}
