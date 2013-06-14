package edu.chop.cbmi.dataExpress.backends.file

import java.io.File
import edu.chop.cbmi.dataExpress.dataModels.{DataType, DataRow, ColumnNameGenerator}
import edu.chop.cbmi.dataExpress.dataModels.sql._
import scala.io.Source
import scala.annotation.tailrec
import scala.collection.immutable
import scala.util.matching.Regex
import java.util.{Date, Calendar}

class MagicOptions{
  //this value is added to the length of the decimal portion inspected by the type inference scheme
  val floatTypePrecisionPadding : Int = 4

  //used as threshold for precision by type inference scheme
  val flotaTypeMinPrecision : Int = 0

  //max number of lines to search
  val maxDepth : Int = 100

  //delimiter for parsing lines
  val delimiter : String = ","

  //source File encoding
  val encoding = "UTF-8"

  //if this value is true, a column with 1 and 0 only entries will be considered to represent booleans
  val treat01AsBoolean = true

  //if treat01AsBoolean is true, this value represents the minimum number of rows that must be inspected
  //to allow boolean over integer precedence. E.g. if this value is 100, and there are only 10 rows in the file
  //then even if if all 10 rows in this column are 0 or 1, the column will be treated as an Integer column
  val boolOverIntThreshold = maxDepth

  //default minimum varchar width
  val minimumVarCharWidth = 100

  //any non-matched string entries with length greater than maximumCharWidth are treated as TextDataTypes
  val maximumVarCharWidth = 500

  //when a varchar pattern is detected, the varchar width is set to the length of the match times this scale factor
  //e.g. if the matched string is "string" with length 6, the varchar width is set to 6*varCharScaleFactor. For a given
  //varChar column the max(maximum observed width, minimumVarCharWidth) is used for the column's varCharWidth
  val varCharScaleFactor = 1.5
}

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/16/13
 * Time: 12:37 PM
 */
sealed case class MagicMarshaller(sourceFile: File, cngOption:Option[ColumnNameGenerator] = None, options: MagicOptions = new MagicOptions)
  extends Marshaller(cngOption.getOrElse(HeaderRowColumnNames(sourceFile, options.delimiter, options.encoding))){

  lazy val typesFromRegex = {
    val lines = {
      val temp = Source.fromFile(sourceFile).getLines().drop(1)
      if(temp.isEmpty)Source.fromFile(sourceFile).getLines() //only one line in the file
      else temp  //skip first line assume it's header
    }
    bldMagicContainer(lines, MagicContainer(List[List[String]](), options)).doMagic()
  }

  private lazy val delimiter = options.delimiter
  private lazy val maxDepth = options.maxDepth
  private lazy val delimiterRegex = s"$delimiter".r
  private lazy val expandedDelimiter = s" $delimiter "

  lazy val types = typesFromRegex.map{_.dataType}

  @tailrec
  private def bldMagicContainer(lines : Iterator[String], container: MagicContainer, count:Int = 1) : MagicContainer = {
    if(lines.hasNext){
      //parse the line
      val split = delimiterRegex.split(delimiterRegex.replaceAllIn(lines.next, expandedDelimiter)).map{s => s.trim}
      val newValues : List[List[String]]= if(container.columnValues.isEmpty){
        split.map{s => List(s)}.toList
      }else{
        container.columnValues.zip(split).map{tpl => tpl._1.:+(tpl._2)}.toList
      }
      if(count==maxDepth || !lines.hasNext)MagicContainer(newValues, options)
      else bldMagicContainer(lines, MagicContainer(newValues, options), count + 1)
    }else container
  }

  def dataTypes() = types

  private def convertToType(i:Int, s: String) = {
    val ts = s.trim
    if(ts.length==0)null
    else typesFromRegex(i).toType(ts)
  }

  def unmarshall(line:String) = {
    val items : Array[String] = delimiterRegex.split(delimiterRegex.replaceAllIn(line,expandedDelimiter)).map{s=>s.trim}
    val convertedItems: Array[Any] = Array.tabulate(items.length){i => convertToType(i,items(i))}
    val rowEntries = col_names.zip(convertedItems)
    DataRow(rowEntries: _*)
  }

  def marshall(row: DataRow[_]) = {
    (""/:row){(s,o) =>
      o match{
        case Some(v) => s"$s${v.toString}$delimiter"
        case _ => s"$s$delimiter"
      }
    }.dropRight(1)
  }

  override def marshallHeader(row:DataRow[String]) : String  = ((""/:row){(h,cn)=> s"$h${cn.getOrElse("")}$delimiter"}).dropRight(1)
}

trait KnownRegex{
  def matches_?(s:String) : Boolean
}

trait TypeFromRegex{

  def dataType() : DataType

  def toType(s:String) : Any

  def toString(a:Any)  = a.toString
}

object TextTypeFromRegex extends TypeFromRegex with KnownRegex{

  def matches_?(s:String) = true //always matches

  def dataType = TextDataType

  def toType(s:String) = s
}

case class VarCharTypeFromRegex(length: Int) extends TypeFromRegex{
  def dataType = CharacterDataType(length, false)

  def toType(s:String)=s
}

private object UnResolvedDateType extends TypeFromRegex{
  def dataType = TextDataType
  def toType(s:String) = s
}


/**
 * checks for matches against different date format regex. It is very conservative. For example, given the
 * input 1900/01/01  the formats yyyy/mm/dd and yyyy/dd/mm provide a match. In this case, the matches_? function
 * returns false. For 1900/01/13, this can only match yyyy/mm/dd and matches_? returns true.
 */
object DateRegex extends KnownRegex{
  //year month day patterns//
  private val delimiters = List("/","-","\\\\","_","\\.","\\s+","#")
  private val yyyymmdd = delimiters.map{del=> "^(\\d\\d\\d\\d)%s(0[1-9]|1[012])%s(0[1-9]|[12][0-9]|3[01])$".format(del,del).r}
  //day month year patterns//
  private val ddmmyyyy = delimiters.map{del=> "^(0[1-9]|[12][0-9]|3[01])%s(0[1-9]|1[012])%s(\\d\\d\\d\\d)$".format(del,del).r}
  //month day year patterns//
  private val mmddyyyy = delimiters.map{del=> "^(0[1-9]|1[012])%s(0[1-9]|[12][0-9]|3[01])%s(\\d\\d\\d\\d)$".format(del,del).r}

  private trait Format
  private object yyyymmddFormat extends Format
  private object ddmmyyyyFormat extends Format
  private object mmddyyyyFormat extends Format

  private def numericFormatConverter(r:Regex, format:Format)(s:String) = {
    val (year,month,day) = format match{
      case `yyyymmddFormat` => {
        val r(y,m,d) = s
        (y,m,d)
      }
      case `ddmmyyyyFormat` => {
        val r(d,m,y) = s
        (y,m,d)
      }
      case `mmddyyyyFormat` =>{
        val r(m,d,y) = s
        (y,m,d)
      }
      case _ => throw new Exception(s"Unknown format $format")
    }
    val cal = Calendar.getInstance()
    cal.set(year.toString.toInt, month.toString.toInt, day.toString.toInt)
    cal.getTime
  }

  private val allPatterns: List[Regex] = yyyymmdd ::: ddmmyyyy ::: mmddyyyy

  private def all_matches(s:String) = allPatterns.map{pat => if(pat.findAllMatchIn(s).isEmpty)None else Some(pat)}.flatten

  /**
   * only returns true if there is unique pattern match
   * @param s
   * @return
   */
  def matches_?(s:String) = all_matches(s).length > 0

  /**
   * returns Some(pattern) if there is a unique match, None otherwise
   * @param s
   * @return
   */
  def matchingPattern(s:String) = {
    val matches = allPatterns.map{pat => if(pat.findAllMatchIn(s).isEmpty)None else Some(pat)}.flatten
    if(matches.length==1)Some(matches.head)
    else if(matches.length>1)Some(UnResolvedDateType)
    else None
  }

  def converterFor(pattern: Regex) : Option[String=>Date] = {
    val format = {
      if(yyyymmdd.contains(pattern))Some(yyyymmddFormat)
      else if(mmddyyyy.contains(pattern))Some(mmddyyyyFormat)
      else if(ddmmyyyy.contains(pattern))Some(ddmmyyyyFormat)
      else None
    }
    format match{
      case Some(f) => Some(numericFormatConverter(pattern,f) _)
      case _ => None
    }
  }
}

case class DateTypeFromRegex(converter: String => Date, pattern:Regex) extends TypeFromRegex{
  def dataType() = DateDataType

  def toType(s:String) = converter(s)
}

object IntTypeFromRegex extends TypeFromRegex with KnownRegex{
  val REGX = """^([-])?(\d+)$""".r

  def matches_?(s:String) = !(REGX.findAllIn(s).isEmpty)

  def dataType = IntegerDataType

  def toType(s:String) = s.toInt
}

case class FloatTypeFromRegex(precision : Int) extends TypeFromRegex{
  def dataType = FloatDataType(precision)

  def toType(s:String) = s.toDouble

}

object FloatRegex extends KnownRegex{
  val REGX = """^([-+])?(\d*)?\.(\d*)?$""".r

  def matches_?(s:String) = REGX.findFirstIn(s) match{
    case Some(s) => s!="."
    case _ => false
  }

  def precision(s:String) = REGX.findFirstIn(s) match{
    case Some(s) => if(s==".") None else{
      val REGX(sgn,i,d) = s
      if(d==null) Some(0) else Some(d.length)
    }
    case _ => None
  }
}

object BooleanTypeFromRegex extends TypeFromRegex with KnownRegex{
  val REGX = """^(true)$|^(false)$|^(0)$|^(1)$|^(t)$|^(f)$""".r

  def matches_?(s:String) = !(REGX.findAllIn(s).isEmpty)

  def dataType = BooleanDataType

  def toType(s:String) = {
    val lc = s.toLowerCase
    if(lc=="true" || lc=="1" || lc=="t")true
    else false
  }
}




sealed case class MagicContainer(columnValues : List[List[String]], options: MagicOptions) {

  lazy private val minPrecision = options.flotaTypeMinPrecision
  lazy private val precisionPad = options.floatTypePrecisionPadding
  //THIS SHOULD NOT CONTAIN TEXT OR VARCHAR TYPE FROM REGEX
  lazy private val regexPatterns = List(IntTypeFromRegex, FloatRegex, BooleanTypeFromRegex, DateRegex)

  /**
   * infers the type of a specific row,column data point
   * @param v
   * @return
   */
  private def inferType(v: String): Option[TypeFromRegex] = {
    val ts = v.trim.toLowerCase
    if (ts.length == 0) None
    else {
      val matchedPatterns: immutable.Set[TypeFromRegex] = regexPatterns.map{
        kr =>
          if (kr.matches_?(ts)) {
            kr match{
              case IntTypeFromRegex => Some(IntTypeFromRegex)
              case FloatRegex => FloatRegex.precision(ts) match{
                case Some(p) => Some(FloatTypeFromRegex(Math.max(p + precisionPad, minPrecision)))
                case _ => None
              }
              case BooleanTypeFromRegex => Some(BooleanTypeFromRegex)
              case DateRegex => DateRegex.matchingPattern(ts) match {
                case Some(pat) => pat match{
                  case r:Regex => DateRegex.converterFor(r) match {
                    case Some(func) => Some(DateTypeFromRegex(func, r))
                    case _ => None
                  }
                  case UnResolvedDateType => Some(UnResolvedDateType)
                }
                case _ => None
              }
              case _ => throw new Exception("Unknown Regex In inferType")
            }
          } else None
      }.flatten.toSet

      val booleanOverInt_? = options.treat01AsBoolean && columnValues.head.length>=options.boolOverIntThreshold
      if(matchedPatterns.size==1)Some(matchedPatterns.head)
      //NOTE THIS CONSENSUS IS DIFFERENT THAN THE columnTypeConsensus. In particular the prioritization rules are different.
      //e.g. here boolean takes priority over int, but the opposite is true in the columnTypeConsensus
      else if(matchedPatterns==Set(BooleanTypeFromRegex, IntTypeFromRegex)){
        if(booleanOverInt_?)Some(BooleanTypeFromRegex) else Some(IntTypeFromRegex)
      }
      else Some(TextTypeFromRegex)
    } //end else ts.length==0
  }


  /**
   * determines column type from a set of possible types as obtained from types inference from several rows based on consensus logic
   * @param valueList
   * @return
   */
  //private def columnTypeConsensus(types:Set[TypeFromRegex]) : TypeFromRegex = {
  private def columnTypeConsensus(valueList: List[String]) : TypeFromRegex = {
    val types = valueList.map{v => inferType(v)}.flatten.toSet.filterNot(_==UnResolvedDateType)

    if(types.contains(TextTypeFromRegex)){
      //define type as VarChar or TextType
      if(valueList.isEmpty)TextTypeFromRegex
      else{
        val length = (valueList.head.trim.length /: valueList){(l,s) => math.max(l,s.trim.length)}
        val adjustedLength = math.max(options.minimumVarCharWidth, length*options.varCharScaleFactor).toInt
        if(adjustedLength>options.maximumVarCharWidth)TextTypeFromRegex
        else VarCharTypeFromRegex(adjustedLength)
      }
    }
    else if(types.size==1)types.head
    else{
      (types.head /: types.tail){(current, next) =>
        current match{
          case IntTypeFromRegex => next match{
            case ftr:FloatTypeFromRegex => next
            case _ => current //int takes priority over everything except float, varchar, text
          } //end current = IntTypeFromRegex
          case ct: FloatTypeFromRegex => next match{
            case nt: FloatTypeFromRegex => if (nt.precision>ct.precision) nt else ct
            case _ => ct //float type takes priority over everything except TEXT and VARCHAR
          }//end current = FloatType
          case BooleanTypeFromRegex => next //if any row is something other than boolean than use that type
          case ct: DateTypeFromRegex => next match{
            case nt: DateTypeFromRegex => if(ct.pattern==nt.pattern)ct
              else TextTypeFromRegex //this case implies two different date formats in the column, treat as text
            case _ => next //if the row has something other than DateType then use that type
          } //end current = DateType
          case _ => TextTypeFromRegex
        }
      }
    }
  }

  /**
   * attempts to determine the types for each column by inferring types across several rows and getting a consensus
   * @return
   */
  def doMagic() : Seq[TypeFromRegex] = columnValues.map{valueList =>
    //columnTypeConsensus(valueList.map{v => inferType(v)}.flatten.toSet)
    columnTypeConsensus(valueList)
  }

}
