package pl.edu.agh

import java.util.concurrent.atomic.AtomicBoolean

import com.amazonaws.auth.{
  AWSCredentials,
  AWSStaticCredentialsProvider,
  BasicAWSCredentials,
  BasicSessionCredentials
}
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model.Message
import javax.annotation.concurrent.GuardedBy
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{
  InputPartition,
  PartitionReader,
  PartitionReaderFactory
}
import org.apache.spark.sql.execution.streaming.LongOffset
import pl.edu.agh.model.OpenAQMessage
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import pl.edu.agh.model.Parser

case class SQSMicroBatchStreamer(
    numPartitions: Int,
    accessKey: String,
    secretKey: String,
    maybeToken: Option[String],
    region: String,
    queueUrl: String
) extends MicroBatchStream
    with Logging {

  private var currentOffset = LongOffset(-1)

  private var active = true

  @GuardedBy("this")
  private var lastOffsetCommitted: LongOffset = LongOffset(-1L)

  private val initialized: AtomicBoolean = new AtomicBoolean(false)

  @GuardedBy("this")
  private val batches = new ListBuffer[(OpenAQMessage, Long)]

  private def initialize(): Unit = {
    val sqsBuilder =
      AmazonSQSClientBuilder
        .standard()
    sqsBuilder
      .setRegion(region)
    sqsBuilder
      .setCredentials(
        new AWSStaticCredentialsProvider(
          maybeToken match {
            case Some(token) =>
              new BasicSessionCredentials(accessKey, secretKey, token)
            case None => new BasicAWSCredentials(accessKey, secretKey)
          }
        )
      )

    val sqs = sqsBuilder.build()

    val messageReader = new Thread(() => {
      while (active) {
        val messages: Seq[Message] =
          sqs.receiveMessage(queueUrl).getMessages.asScala
        val orderedMessages = messages
          .filter(x => Parser(x.getBody).isRight)
          .map(message => {
            currentOffset = currentOffset.+(1)
            (s"${message.getBody}", currentOffset.offset)
          })

        batches.appendAll(
          orderedMessages.map(x => (Parser(x._1).toOption.get, x._2))
        )
        messages.foreach(m => sqs.deleteMessage(queueUrl, m.getReceiptHandle))
        Thread.sleep(100)
      }
    })
    messageReader.start()
  }

  override def planInputPartitions(
      start: Offset,
      end: Offset
  ): Array[InputPartition] = {
    val startOrdinal = start.asInstanceOf[LongOffset].offset.toInt + 1
    val endOrdinal = end.asInstanceOf[LongOffset].offset.toInt + 1

    // Internal buffer only holds the batches after lastOffsetCommitted
    val rawList = synchronized {
      if (initialized.compareAndSet(false, true)) {
        initialize()
      }

      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1

      batches.slice(sliceStart, sliceEnd)
    }

    val slices =
      Array.fill(numPartitions)(new ListBuffer[(OpenAQMessage, Long)])

    rawList.zipWithIndex.foreach {
      case (r, idx) =>
        slices(idx % numPartitions).append(r)
    }

    slices.map(SQSInputPartition)
  }

  override def createReaderFactory(): PartitionReaderFactory =
    (partition: InputPartition) => {
      val slice = partition.asInstanceOf[SQSInputPartition].slice
      new PartitionReader[InternalRow] {
        private var currentIdx = -1

        override def next(): Boolean = {
          currentIdx += 1
          currentIdx < slice.size
        }

        override def get(): InternalRow = {
          InternalRow(slice(currentIdx)._1, slice(currentIdx)._2)
          encodeMessage(slice(currentIdx)._1)
        }

        override def close(): Unit = {}
      }
    }

  private def encodeMessage(message: OpenAQMessage): InternalRow = {
    val messageEncoder = Encoders.product[OpenAQMessage]
    val messageExprEncoder =
      messageEncoder.asInstanceOf[ExpressionEncoder[OpenAQMessage]]
    messageExprEncoder.createSerializer()(message)
  }

  override def stop(): Unit = {
    active = false
  }

  override def commit(end: Offset): Unit =
    synchronized {
      val newOffset = end.asInstanceOf[LongOffset]

      val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

      if (offsetDiff < 0) {
        sys.error(
          s"Offsets committed out of order: $lastOffsetCommitted followed by $end"
        )
      }

      batches.trimStart(offsetDiff)
      lastOffsetCommitted = newOffset
    }

  override def latestOffset(): Offset = currentOffset

  override def initialOffset(): Offset = LongOffset(-1)

  override def deserializeOffset(json: String): Offset = {
    LongOffset(json.toLong)
  }
}

case class SQSInputPartition(slice: ListBuffer[(OpenAQMessage, Long)])
    extends InputPartition
