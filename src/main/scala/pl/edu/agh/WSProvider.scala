package pl.edu.agh

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class WSProvider extends DataSourceRegister with TableProvider with Logging {

  override def shortName(): String = "ws"

  private val schema = StructType(
    StructField("type", StringType, true) ::
    StructField("trade_id", LongType, true) ::
    StructField("sequence", LongType, true) ::
    StructField("time", StringType, true) ::
    StructField("product_id", StringType, true) ::
    StructField("price", DoubleType, true) ::
    StructField("side", StringType, true) ::
    StructField("last_size", DoubleType, true) ::
    StructField("best_bid", DoubleType, true) ::
    StructField("best_ask", DoubleType, true) :: Nil
  )

  override def getTable(
      x: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    assert(partitioning.isEmpty)
    new WSStreamer(schema, SparkSession.active.sparkContext.defaultParallelism)
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    schema
  }
}
