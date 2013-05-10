package edu.chop.cbmi.dataExpress.test.backends.file

import org.scalatest.{BeforeAndAfter, GivenWhenThen, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import edu.chop.cbmi.dataExpress.backends.file.{CustomMarshaller, StaticMarshaller, DelimiterMarshaller}
import edu.chop.cbmi.dataExpress.dataModels.{ColumnNameGenerator, DataRow, SeqColumnNames}
import edu.chop.cbmi.dataExpress.dataModels.RichOption._

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
      val content = List("Bob,249,M","Jane Doe,3430,F","Mike R.,,M","Steve,83839,")
      val cng = SeqColumnNames(Seq("Name","ID","Gender"))
      val marshaller = kind match{
        case DELIMITER => DelimiterMarshaller(",",cng)
        case STATIC => StaticMarshaller(SeqColumnNames(Seq("LINE")))
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
    val cg = SeqColumnNames(Seq("LINE"))

    def unmarsh(cng:ColumnNameGenerator)(line:String) = {
      DataRow((cng.generate_column_names().head,line.toUpperCase()))
    }

    def marsh(cng:ColumnNameGenerator)(dr: DataRow[_]) = {
      dr(cng.generate_column_names().head).asu[String].toLowerCase
    }

    val cm = CustomMarshaller(cg, unmarsh(cg) _, marsh(cg) _ )

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
