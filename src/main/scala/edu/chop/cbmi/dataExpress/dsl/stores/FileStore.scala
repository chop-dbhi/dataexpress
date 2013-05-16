package edu.chop.cbmi.dataExpress.dsl.stores

import edu.chop.cbmi.dataExpress.backends.file.{FileBackend}
import edu.chop.cbmi.dataExpress.dataModels.ColumnNameGenerator

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/10/13
 * Time: 1:08 PM
 */
case class FileStore(fb: FileBackend,cng: ColumnNameGenerator, writeHeaderOnCreate: Boolean = true) extends Store{
  private var _unique_id : Any = fb.file.getAbsolutePath

  override def is_closed_? : Boolean = !fb.is_open_? //really no such thing

  override def open = true

  override def close: Boolean = fb.close

  override def save : Boolean = true //really no such thing

  override def set_unique_id(id: Any) = {
    _unique_id = id
  }

  override def unique_id = _unique_id
}
