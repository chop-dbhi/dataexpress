package edu.chop.cbmi.dataExpress.dataModels.file

import edu.chop.cbmi.dataExpress.backends.file.FileBackend
import edu.chop.cbmi.dataExpress.dataModels.DataRow

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/10/13
 * Time: 9:51 AM
 * To change this template use File | Settings | File Templates.
 */
case class FileTableColumn[G] private[file](private val be: FileBackend, columnName: String,f: Option[Any]=>G) extends Iterator[G]{
  private val iterator: Iterator[DataRow[_]] = be.read()

  override def next() = {
    val r = iterator.next().apply(columnName)
    f(r)
  }

  override def hasNext = iterator.hasNext
}
