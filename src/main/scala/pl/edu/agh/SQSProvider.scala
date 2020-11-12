package pl.edu.agh

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SQSProvider extends DataSourceRegister with TableProvider with Logging {

  override def shortName(): String = "sqs"

  private val schema = StructType(
    StructField(
      "date",
      StructType(
        List(
          StructField("local", StringType, true),
          StructField("utc", StringType, true)
        )
      ),
      true
    ) ::
      StructField("parameter", StringType, true) ::
      StructField("value", DoubleType, true) ::
      StructField("unit", StringType, true) ::
      StructField(
        "averagingPeriod",
        StructType(
          List(
            StructField("unit", StringType, nullable = true),
            StructField("value", DoubleType, true)
          )
        ),
        true
      ) ::
      StructField("location", StringType, true) ::
      StructField("city", StringType, true) ::
      StructField("country", StringType, true) ::
      StructField(
        "coordinates",
        StructType(
          List(
            StructField("latitude", DoubleType, true),
            StructField("longitude", DoubleType, true)
          )
        ),
        true
      ) :: Nil
  )

  override def getTable(
      x: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    assert(partitioning.isEmpty)
    new SQSStreamer(schema, SparkSession.active.sparkContext.defaultParallelism)
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    schema
  }
}
