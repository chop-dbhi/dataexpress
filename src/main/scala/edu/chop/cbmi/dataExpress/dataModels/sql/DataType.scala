package edu.chop.cbmi.dataExpress.dataModels.sql

import edu.chop.cbmi.dataExpress.dataModels.DataType


case object TinyIntegerDataType extends DataType
case object SmallIntegerDataType extends DataType
case object IntegerDataType extends DataType
case object BigIntegerDataType extends DataType
case class FloatDataType(precision: Int) extends DataType
case class DecimalDataType(precision: Int, scale: Int) extends DataType

case object DateDataType extends DataType
case class DateTimeDataType(withZone: Boolean) extends DataType
case class TimeDataType(withZone: Boolean) extends DataType

case class CharacterDataType(length: Int, fixedWidth:Boolean) extends DataType
//case class BitDataType(val length:Int, val fixedWidth:Boolean) extends DataType {}
case object BooleanDataType extends DataType
case object BitDataType extends DataType
case object TextDataType extends DataType
case object BigBinaryDataType extends DataType