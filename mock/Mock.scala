// using scala 3.0.2
// using lib org.java-websocket:Java-WebSocket:1.5.2
// using lib org.slf4j:slf4j-simple:1.7.25

import scala.util._
import scala.io.StdIn.readLine
import java.net.InetSocketAddress
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer
import java.nio.channels.SelectionKey
import collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import java.time.temporal.ChronoUnit.{SECONDS, MILLIS, MINUTES}

object Mock extends App:
  val s = new WebSocketServer(new InetSocketAddress("0.0.0.0", 8025)):
    override def onConnect(key: SelectionKey): Boolean = true
    override def onOpen(webSocket: WebSocket, clientHandshake: ClientHandshake): Unit =
      println("Opening connection...")
      webSocket.send("""{"type":"subscriptions","channels":[{"name":"ticker","product_ids":["ETH-BTC","ETH-USD"]}]}""")
      println("Opened connection successfully!")
    override def onClose(webSocket: WebSocket, code: Int, reason: String, remote: Boolean): Unit = println("Conection has been closed.")
    override def onError(webSocket: WebSocket, ex: Exception): Unit = println(s"Error $ex")
    override def onMessage(webSocket: WebSocket, msg: String): Unit = println("Received message for subscription")
    override def onStart(): Unit = println("Starting...")
  s.start()

  val now = java.time.Instant.parse("2021-11-01T12:00:00.123123Z")

  def msg(ts: String, price: Double) =
    s"""{"type":"ticker","sequence":0,"product_id":"ETH-USD","price":"$price","open_24h":"0","volume_24h":"0","low_24h":"0","high_24h":"0","volume_30d":"0","best_bid":"0","best_ask":"0","side":"buy","time":"$ts","trade_id":0,"last_size":"0"}"""

  def sendFrames(list: List[String]) = 
    def price() = scala.util.Random.nextDouble() * 1000
    list.foreach { ts =>
      val p = price()
      println(s"$ts - $p")
      s.broadcast(msg(ts, p))
    }

  def step(str: String, list: List[String] = Nil) =
    println(str)
    readLine()
    sendFrames(list)

  def diff(n: Int) = now.plus(n, SECONDS).toString

  val l1 = List(now.toString, diff(14), diff(7))
  step("Press ENTER to send the first message with T0, T0+14s, T0+7s timestamps.", l1)

  val l2 = List(diff(15), diff(8), diff(21))
  step("Press ENTER to send second messages: with T0+15s, T0+8s, T0+21s timestamps", l2)

  val l3 = List(diff(4), diff(17))
  step("Press ENTER to send second messages: with T0+4s, T0+17s timestamps", l3)

  step("Press ENTER to close WebSocket and exit.")

  s.getConnections().asScala.foreach(_.close(1000))
  s.stop(1000)
