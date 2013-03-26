package edu.chop.cbmi.dataExpress.backends

import au.com.bytecode.opencsv._
import java.io.FileReader
import java.io.FileWriter

/*
This is a base class that all Backends will inherit from. It was necessary to crate this when flat file types were cr
introduced that do not abide by the the same interface as a SQL database.
 */
class FileBackend(val filename:String, val delimiter:Character = ',') {

  var readHandle:Option[CSVReader] = None
  var writeHandle:Option[CSVWriter] = None

  def openForRead(){
      this.readHandle = Option(new CSVReader(new FileReader(filename), delimiter))
  }

  def openForWrite(){
      this.writeHandle = Option(new CSVWriter(new FileWriter(filename), delimiter))
  }

  def close() {

      readHandle match {
        case Some(reader) => reader.close()
        case None =>
      }
      writeHandle match {
        case Some(writer) => writer.close()
        case None =>
      }

      readHandle = None
      writeHandle = None
  }

}