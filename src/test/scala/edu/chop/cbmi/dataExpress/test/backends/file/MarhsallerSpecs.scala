package edu.chop.cbmi.dataExpress.test.backends.file

import org.scalatest.{BeforeAndAfter, GivenWhenThen, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import edu.chop.cbmi.dataExpress.backends.file._
import edu.chop.cbmi.dataExpress.dataModels._
import edu.chop.cbmi.dataExpress.dataModels.RichOption._
import edu.chop.cbmi.dataExpress.backends.file.DelimiterMarshaller
import scala.Some
import edu.chop.cbmi.dataExpress.backends.file.CustomMarshaller
import edu.chop.cbmi.dataExpress.backends.file.StaticMarshaller
import edu.chop.cbmi.dataExpress.backends.file.DelimiterMarshaller
import scala.Some
import edu.chop.cbmi.dataExpress.backends.file.DoubleDelimiterMarshaller
import edu.chop.cbmi.dataExpress.backends.file.StaticMarshaller
import edu.chop.cbmi.dataExpress.dataModels.sql.TextDataType

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/9/13
 * Time: 12:58 PM
 */
class MarhsallerSpecs extends FunSpec with GivenWhenThen with ShouldMatchers with BeforeAndAfter{
  val CUSTOM = 'CUSTOM
  val DELIMITER = 'DELIMITER
  val STATIC = 'STATIC
  val REGEX = 'REGEX


  def fixture(kind:Symbol) = {
    new {
      val content = List("Bob,249,M","Jane Doe,3430,F","Mike R.,,M","Steve,83839,",",,",",83,")
      val cng = Seq("Name","ID","Gender")
      val marshaller = kind match{
        case DELIMITER => DelimiterMarshaller(",",cng)
        case STATIC => StaticMarshaller(Seq("LINE"))
        case _ => throw new Exception("Unknown Line Marshaller")
      }
    }
  }

  describe("A Delimiter Marshaller"){
    val f = fixture(DELIMITER)
    it("can convert value delimited data to DataRows"){
      val rows: List[DataRow[_]] = f.content.map{line =>  f.marshaller.unmarshall(line)}
      rows.length should equal(f.content.length)

      rows.zip(f.content).foreach{tpl =>
        val (dr,r) = tpl
        val ns = dr.Name.getOrElse("")
        val ids = dr.ID.getOrElse("")
        val gs = dr.Gender.getOrElse("")
        r should equal(s"$ns,$ids,$gs")
      }

      And("It can convert DataRows back to value delimited data")
      val lines = rows.map{row => f.marshaller.marshall(row)}
      lines.zip(f.content).foreach{pair => pair._1 should equal(pair._2) }
    }

  }

  describe("A Typed Marshaller"){
    it("converts value delimited data to DataRows[TYPE]"){
      val numbers = List("1,2,3", "6,7.0,")
      val mar = DoubleDelimiterMarshaller(",",Seq("A","B","C"))
      val rows = numbers.map{line => mar.unmarshall(line)}
      rows.length should equal(numbers.length)
      (0 /: rows){(idx, row) =>
        val rowSum = (0.0/:row){(s,o) =>
          o.as[Double] match{
            case Some(n) => s + n
            case _ => s
          }
        }
        val lineSum = (0.0 /: numbers(idx).split(",")){(s,ns)=>
          if(ns.trim.length>0) s + ns.trim.toDouble
          else s
        }
        rowSum should equal(lineSum)
        idx + 1
      }
    }
  }

  describe("A Static Marshaller"){
    val f = fixture(STATIC)
    it("simply stores each line as the single column in a data row"){
      val rows = f.content.map{line => f.marshaller.unmarshall(line)}
      rows.zip(f.content).foreach{tpl =>
        val(dr,l1) = tpl
        dr.LINE.getOrElse("") should equal(l1)
      }
    }
  }

  describe("A Custom Marshaller"){
    val f = fixture(STATIC)
    val cg = Seq("LINE")

    def unmarsh(columns:Seq[String])(line:String) = {
      DataRow((columns.head,line.toUpperCase()))
    }

    def marsh(columns:Seq[String])(dr: DataRow[_]) = {
      dr(columns.head).asu[String].toLowerCase
    }

    def types(cn:String) : DataType = {
      TextDataType
    }

    val cm = new CustomMarshaller(cg, unmarsh(cg) _, marsh(cg) _ , types )

    it("should convert lines to DataRows with whatever the custom unmarsh function does"){
      val rows = f.content.map{line => cm.unmarshall(line)}
      rows.zip(f.content).foreach{tpl =>
        val (dr,l1) = tpl
        dr.LINE.asu[String] should equal(l1.toUpperCase())
      }
      And("It should convert the DataRows to whatever the custom marsh function does")
        val lines = rows.map{row => cm.marshall(row)}
        lines.zip(f.content).foreach{pair => pair._1 should equal(pair._2.toLowerCase)}
    }
  }

}
