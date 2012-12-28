package edu.chop.cbmi.dataExpress.dataModels.sql

import edu.chop.cbmi.dataExpress.dataModels.DataType


case class TinyIntegerDataType() extends DataType{}
case class SmallIntegerDataType() extends DataType{}
case class IntegerDataType() extends DataType {}
case class FloatDataType(precision: Int) extends DataType {}
case class DecimalDataType(precision: Int, scale: Int) extends DataType {}

case class DateDataType() extends DataType {}
case class DateTimeDataType(withZone: Boolean) extends DataType {}
case class TimeDataType(withZone: Boolean) extends DataType {}

case class CharacterDataType(length: Int, fixedWidth:Boolean) extends DataType {}
//case class BitDataType(val length:Int, val fixedWidth:Boolean) extends DataType {}
case class BooleanDataType() extends DataType {}
case class BitDataType() extends DataType {}
case class TextDataType() extends DataType {}
case class BigBinaryDataType() extends DataType {}