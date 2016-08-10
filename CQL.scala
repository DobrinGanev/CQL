package cassandra

/**
  * Created by dobrin on 06/08/16.
  */

import com.datastax.spark.connector.{ColumnRef, _}
import com.datastax.spark.connector.cql.{CassandraConnector, ColumnDef, PartitionKeyColumn}
import org.apache.spark.SparkContext

object CQL {

  def createKeyspace(keyspace: String, withOptions: String = "WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}", sc: SparkContext) = {
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.executeAsync("CREATE KEYSPACE IF NOT EXISTS %s %s".format(keyspace, withOptions))
    }
  }

  def dropKeyspace(keyspace: String, sc: SparkContext) = {
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.executeAsync("DROP KEYSPACE %s ;".format(keyspace))
    }
  }

  def dropTable(keyspace: String,tableName: String, sc: SparkContext) = {
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.executeAsync("DROP TABLE %s.%s ;".format(keyspace, tableName))
    }
  }

  def createTable(tableName: String, columns: String, primaryKey: String, sc: SparkContext) = {
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.executeAsync("CREATE TABLE IF NOT EXISTS %s \n(%s, \nPRIMARY KEY (%s));".format(tableName, columns, primaryKey))
    }
  }


  def queryExecute(sc: SparkContext, queryString: String) = {
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.executeAsync(queryString);
    }
  }

  def selectToRDD(sc: SparkContext, keyspace: String, tableName: String, columns: Seq[ColumnName]) = {
    sc.cassandraTable(keyspace, tableName).select(columns: _*)
  }
}
