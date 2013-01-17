package edu.chop.cbmi.dataExpress.dataModels
import scala.reflect.Manifest
import edu.chop.cbmi.dataExpress.dataModels.RichOption._
import edu.chop.cbmi.dataExpress.exceptions.ColumnDoesNotExist
import DataRow.map_to_option
import edu.chop.cbmi.dataExpress.exceptions.ColumnDoesNotExist

/**
 * A simple immutable [[edu.chop.cbmi.dataExpress.dataModels.DataTable]] that maintains all data elements in memory
 */
case class SimpleDataTable[+T](columnNames: Seq[String])(private val data: Seq[Seq[T]])
  extends DataTable[T] with Iterator[DataRow[T]] {
  private var index = 0
  require(data.length > 0, println("data cannot be empty"))
  require(columnNames.length == data(0).length, println("generate_column_names.length must equal data(0).length"))
  require((true /: data)((b: Boolean, l: Seq[_]) => b && l.length == data(0).length),
    println("All elements in data must be of equal length"))
  private val  iterator = SimpleDataIterator(columnNames, data)

  lazy val dataTypes = {
    Seq[DataType]()
  }

  val columnCount = columnNames.length

  
   /**
   * @param idx index of desired table row
   * @return DataRow[Option[T]] containing the elements of row idx wrapped in an Option
   */
   def apply(idx: Int) = DataRow(columnNames)(map_to_option(data(idx)))

  /**
   * @return int the number of rows
   */
  override def length = data.length

  override def hasNext = iterator.hasNext
  
  override def next = iterator.next()

  override def col(name: String) : Iterator[Option[T]] = if(hasColumn(name)) {
    val idx = columnNames.indexOf(name)
    data.iterator.map((l: Seq[T]) => if (l(idx) == null) None else Some(l(idx)))
  } else throw ColumnDoesNotExist(name)

  override def col_as[G](name: String)(implicit m: Manifest[G]) : Iterator[Option[G]] = if(hasColumn(name)) {
    val idx = columnNames.indexOf(name)
    data.iterator.map((l: Seq[T]) => if (l(idx) == null) None else Some(l(idx)).as[G])
  } else throw ColumnDoesNotExist(name)

  override def col_asu[G](name: String)(implicit m: Manifest[G]) : Iterator[G] = if(hasColumn(name)) {
    val idx = columnNames.indexOf(name)
    data.iterator.map((l: Seq[T]) => if (l(idx) == null) None.asu[G] else Some(l(idx)).asu[G])
  } else throw ColumnDoesNotExist(name)

  override def selectDynamic(name: String): Iterator[Option[T]] = {
    if (hasColumn(name)) this.col(name)
    else throw ColumnDoesNotExist(name)
  }

  case class SimpleDataIterator[+T] private[SimpleDataTable]
  (private val column_names: Seq[String], private val data: Seq[Seq[T]]) extends Iterator[DataRow[T]] {

    private var index = 0

    def hasNext() = index < data.length

    def next(): DataRow[T] = {
      index += 1
      DataRow(column_names)(map_to_option(data(index-1)))
    }
  }

}
