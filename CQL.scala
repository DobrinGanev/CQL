package cassandra

/**
  * Created by dobrin on 06/08/16.
  */

import com.datastax.spark.connector.{ColumnRef, _}
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.CassandraRDD
import com.datastax.spark.connector.types.{BigIntType, IntType, TextType}
import org.apache.spark.SparkContext

object CQL {

  def createKeyspace(keyspace: String, withOptions: String = "WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}", sc: SparkContext) = {
    val queryString =
      s""" CREATE KEYSPACE IF NOT EXISTS
          |$keyspace
          |$withOptions
        """.stripMargin
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(queryString)
    }
  }

  def dropKeyspace(keyspace: String, sc: SparkContext) = {
    val queryString =
      s"""DROP KEYSPACE IF EXISTS
          |$keyspace
        """.stripMargin
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(queryString)
    }
  }

  def dropTable(keyspace: String, tableName: String, sc: SparkContext) = {
    val queryString =
      s"""DROP TABLE IF EXISTS
          |$keyspace
          |.
          |$tableName ;
        """.stripMargin
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(queryString)
    }
  }

  def createTable(tableName: String, columns: List[String], primaryKey: List[String], sc: SparkContext) = {
    val col = columns.mkString(",\n")
    val keys = primaryKey.mkString(",")
    val queryString =
      s"""CREATE TABLE IF NOT EXISTS $tableName (
          |$col,
          |PRIMARY KEY ($keys));
        """.stripMargin
    println(queryString)
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.executeAsync(queryString)
    }
  }

  def createTableWithClusteringColumns(tableName: String, columns: List[String], primaryKey: List[String], clusteringColumns: List[String], sc: SparkContext) = {
    val col = columns.mkString(",\n")
    val keys = primaryKey.mkString(",")
    val clrColumns = clusteringColumns.mkString(",")
    val queryString =
      s"""CREATE TABLE IF NOT EXISTS $tableName (
          |$col,
          |PRIMARY KEY (($keys),$clrColumns));
        """.stripMargin
    println(queryString)
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.executeAsync(queryString)
    }
  }

  def queryExecute(sc: SparkContext, queryString: String) = {
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(queryString);
    }
  }

  def selectToRDD(sc: SparkContext, keyspace: String, tableName: String, columns: List[ColumnName]) = {
    sc.cassandraTable(keyspace.toLowerCase, tableName.toLowerCase).select(columns: _*)
  }

  def rowCount(sc: SparkContext, keyspace: String, tableName: String) = {
    val table: CassandraRDD[CassandraRow] = sc.cassandraTable(keyspace, tableName)
    table.count()
  }
}
