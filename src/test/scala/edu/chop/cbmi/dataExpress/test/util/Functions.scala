package edu.chop.cbmi.dataExpress.test.util

import java.util.Calendar

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 3/16/12
 * Time: 2:25 PM
 * To change this template use File | Settings | File Templates.
 */

object Functions {
  def sqlDateFrom(yyyymmdd: String) = new java.sql.Date(timeInMillisFrom(yyyymmdd))

  def timeInMillisFrom(yyyymmdd: String): Long = {
    val y = yyyymmdd.substring(0, 4).toInt
    val m = yyyymmdd.substring(4, 6).toInt - 1
    val d = yyyymmdd.substring(6).toInt
    val cal = Calendar.getInstance()
    cal.set(y, m, d)
    cal.getTimeInMillis
  }
}
