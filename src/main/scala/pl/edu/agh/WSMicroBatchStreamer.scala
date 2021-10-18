package pl.edu.agh

import java.util.concurrent.atomic.AtomicBoolean


import javax.annotation.concurrent.GuardedBy
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{
  InputPartition,
  PartitionReader,
  PartitionReaderFactory
}
import java.nio.ByteBuffer
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}
import org.apache.spark.sql.execution.streaming.LongOffset
import pl.edu.agh.model.OpenAQMessage
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import okhttp3._
import okio.ByteString
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import pl.edu.agh.model.Parser

case class WSMicroBatchStreamer(
    numPartitions: Int
) extends MicroBatchStream
    with Logging {

  private var currentOffset = LongOffset(-1)

  private var active = true

  @GuardedBy("this")
  private var lastOffsetCommitted: LongOffset = LongOffset(-1L)

  private val initialized: AtomicBoolean = new AtomicBoolean(false)

  @GuardedBy("this")
  private val batches = new ListBuffer[(OpenAQMessage, Long)]

  @GuardedBy("this")
  protected val messageQueue: BlockingQueue[(OpenAQMessage, Long)] = new ArrayBlockingQueue[(OpenAQMessage, Long)](1000)

  @GuardedBy("this")
  @transient
  var socket: Option[WebSocket] = None

  @GuardedBy("this")
  @transient
  var worker: Option[Thread] = None

  private def initialize(): Unit = synchronized {

    val client = new OkHttpClient.Builder()
      .readTimeout(0, TimeUnit.MILLISECONDS)
      .build()

    val ws = client.newWebSocket(new Request.Builder()
      .url("wss://ws-feed.exchange.coinbase.com")
      .build(), new WebSocketListener {

      override def onOpen(webSocket: WebSocket, response: Response): Unit = {
        log.debug("Opened websocket connection...")
        // Send out initial messages which we will get echoed back
        webSocket.send("""{"type":"subscribe","channels":[{"name":"ticker","product_ids":["ETH-BTC","ETH-USD"]}]}""")
      }

      override def onClosed(webSocket: WebSocket, code: Int, reason: String): Unit = {
        log.info(s"Websocket closed: $reason ($code) ")
        if(socket.isDefined) {
          log.warn("Attempting to reconnect in 1s...")
          Thread.sleep(1000)
          initialize()
        }
      }

      override def onFailure(webSocket: WebSocket, t: Throwable, response: Response): Unit = {
        log.warn(s"Websocket failed: $response\n${t.getMessage}", t)
        if(socket.isDefined) {
          log.warn("Attempting to reconnect in 1s...")
          Thread.sleep(1000)
          initialize()
        }
      }

      override def onMessage(webSocket: WebSocket, str: String): Unit = {
        Parser(str) match {
          case Right(message) => 
            currentOffset = currentOffset.+(1)
            messageQueue.put((message, currentOffset.offset))
          case Left(exception) => 
            log.warn("Expected WsMessage bug got " + exception)
        }
      }
    })

    socket = Some(ws)

    worker = {
      val thread = new Thread("Queue Worker") {
        setDaemon(true)
        override def run(): Unit = {
          while(socket.isDefined) {
            val event = messageQueue.poll(100, TimeUnit.MILLISECONDS)
            if(event != null) {
              batches.append(event)
            }
          }
        }
      }
      thread.start()
      Some(thread)
    }
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

    slices.map(WSInputPartition)
  }

  override def createReaderFactory(): PartitionReaderFactory =
    (partition: InputPartition) => {
      val slice = partition.asInstanceOf[WSInputPartition].slice
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

case class WSInputPartition(slice: ListBuffer[(OpenAQMessage, Long)])
    extends InputPartition
