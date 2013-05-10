package edu.chop.cbmi.dataExpress.backends.file

import java.io.{FileWriter, BufferedWriter, PrintWriter, File}
import scala.io.Source
import edu.chop.cbmi.dataExpress.dataModels.DataRow

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
  def read() : Iterator[DataRow[_]]

  def write(rows: Iterator[DataRow[_]], writeMode: WriteMode) : Boolean

  def write(row: DataRow[_], writeMode: WriteMode) : Boolean

  //CONCRETE STUFF
  lazy val source = Source.fromFile(file)

  def delete() = if(file.exists())file.delete

  def ensureParentDirs() = if(!file.getParentFile.exists())file.getParentFile.mkdirs()

  def makeNewFile() = {
    delete
    ensureParentDirs()
    file.createNewFile()
  }

  def close() = source.close()

}
