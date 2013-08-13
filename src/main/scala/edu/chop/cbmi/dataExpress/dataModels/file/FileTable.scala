package edu.chop.cbmi.dataExpress.dataModels.file

import edu.chop.cbmi.dataExpress.backends.file._
import edu.chop.cbmi.dataExpress.dataModels.{Metadata, DataRow, DataTable}
import edu.chop.cbmi.dataExpress.exceptions.ColumnDoesNotExist
import scala.language.dynamics
import edu.chop.cbmi.dataExpress.dataModels.RichOption._

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/10/13
 * Time: 9:30 AM
 */



sealed case class FileTable private[dataModels](private val be: FileBackend, m:Marshaller)
  extends DataTable[Any]{

  override def dataTypes = be.dataTypes()

  lazy private val iterator = be.read()

  override def columnCount = m.columnCount

  override def columnNames = m.columnNames

  override def next() = iterator.next()

  override def hasNext() = iterator.hasNext

  override def col(name: String) = if(hasColumn(name)){
    def f(item:Option[Any]) = item
    FileTableColumn[Option[Any]](be,name, f)
  }else throw ColumnDoesNotExist(name)

  override def col_as[G](name:String)(implicit m: Manifest[G]) = if(hasColumn(name)){
    def f(a:Option[Any]) = a.as[G]
    FileTableColumn[Option[G]](be, name, f)
  } else throw ColumnDoesNotExist(name)

  override def col_asu[G](name: String)(implicit m: Manifest[G]) = if(hasColumn(name)){
    def f(a:Option[Any]) = a.asu[G]
    FileTableColumn[G](be, name, f)
  }else throw ColumnDoesNotExist(name)
}
