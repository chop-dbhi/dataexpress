/*
Copyright (c) 2012, The Children's Hospital of Philadelphia All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
   disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
   following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.chop.cbmi.dataExpress.dataModels.sql

import edu.chop.cbmi.dataExpress.dataModels.DataType

/**
 * Created by IntelliJ IDEA.
 * User: masinoa
 * Date: 11/9/11
 * Time: 12:19 PM
 * To change this template use File | Settings | File Templates.
 */

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