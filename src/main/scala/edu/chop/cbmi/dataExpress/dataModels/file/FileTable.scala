package edu.chop.cbmi.dataExpress.dataModels.file

import edu.chop.cbmi.dataExpress.backends.file.FileBackend
import edu.chop.cbmi.dataExpress.dataModels.{DataRow, ColumnNameGenerator, DataTable}
import edu.chop.cbmi.dataExpress.exceptions.ColumnDoesNotExist
import scala.language.dynamics
import edu.chop.cbmi.dataExpress.dataModels.RichOption._

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/10/13
 * Time: 9:30 AM
 */
sealed case class FileTable private[dataModels](private val be: FileBackend, cng: ColumnNameGenerator)
  extends DataTable[Any](cng){

  lazy val dataTypes = be.dataTypes()

  lazy private val iterator = be.read()

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
