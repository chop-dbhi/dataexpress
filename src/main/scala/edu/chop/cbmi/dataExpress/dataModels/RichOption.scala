package edu.chop.cbmi.dataExpress.dataModels

import scala.reflect.Manifest
import scala.language.implicitConversions

class RichOption[T] private (private val data:Option[T]){

  def as[G](implicit m : Manifest[G]) : Option[G] = data match{
    case Some(d) => Some(d.asInstanceOf[G])
    case None => None
  }

  def asu[G](implicit m : Manifest[G]) : G = data match{
    case Some(d) => d.asInstanceOf[G]
    case None => throw new Exception("Cannot convert None type to " + m.toString())
  }

}

object RichOption {

  implicit def optionToRichOption[T](opt : Option[T]) = new RichOption(opt)

}