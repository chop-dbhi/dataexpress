package edu.chop.cbmi.dataExpress.dataWriters.file

import edu.chop.cbmi.dataExpress.dataWriters.OperationStatus

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/14/13
 * Time: 10:11 AM
 * To change this template use File | Settings | File Templates.
 */
case class FileOperationStatus(success: Boolean) extends OperationStatus {
  override def operation_succeeded_? : Boolean = success
}
