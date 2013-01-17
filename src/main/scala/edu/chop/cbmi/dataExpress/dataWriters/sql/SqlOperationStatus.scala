package edu.chop.cbmi.dataExpress.dataWriters.sql

import edu.chop.cbmi.dataExpress.dataWriters.OperationStatus
import edu.chop.cbmi.dataExpress.dataModels.DataRow

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 12/21/11
 * Time: 12:20 PM
 * To change this template use File | Settings | File Templates.
 */

case class SqlOperationStatus(private val succeed : Boolean, returned_keys : DataRow[_] = DataRow.empty) extends OperationStatus {
  def operation_succeeded_? = succeed
}