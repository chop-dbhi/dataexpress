package edu.chop.cbmi.dataExpress.dataModels.sql


case class SqlRelationColumn[G] private[sql](private val sql_query_package : SqlQueryPackage, f:(Any)=>G)
  extends Iterator[G]{
  private val query_package = SqlQueryPackage(sql_query_package.dataStore, sql_query_package.query, sql_query_package.bindVars)
  private val iterator = SqlRelationColumnIterator(query_package)
  
  
  override def next() = iterator.next()
  
  override def hasNext = iterator.hasNext()

  case class SqlRelationColumnIterator private[SqlRelationColumn](private val sql_query_package : SqlQueryPackage)
    extends SqlIterator[G](sql_query_package){

    override def generate_next() : G = {
      val item = next_item_in_column(1)
      f(item)
    }
  }
}


