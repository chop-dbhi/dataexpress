package edu.chop.cbmi.dataExpress.dataModels.file

import edu.chop.cbmi.dataExpress.backends.file.FileBackend

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/10/13
 * Time: 9:51 AM
 * To change this template use File | Settings | File Templates.
 */
case class FileTableColumn[G] private[file](private val be: FileBackend, columnName: String,f: Any=>G) extends Iterator[G]{
  private val iterator = be.read()

  override def next() = f(iterator.next())

  override def hasNext = iterator.hasNext
}
