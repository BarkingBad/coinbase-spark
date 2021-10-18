package pl.edu.agh

import java.util

import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  Table,
  TableCapability
}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class WSStreamer(val schema: StructType, numPartitions: Int)
    extends Table
    with SupportsRead {

  override def name(): String = "ws://..."

  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.MICRO_BATCH_READ
    ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    () =>
      new Scan {
        override def readSchema(): StructType = schema

        override def toMicroBatchStream(
            checkpointLocation: String
        ): MicroBatchStream = {
          WSMicroBatchStreamer(
            numPartitions
          )
        }
      }
}
