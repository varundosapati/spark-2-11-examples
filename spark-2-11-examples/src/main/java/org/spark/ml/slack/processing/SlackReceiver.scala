package org.spark.ml.slack.processing

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.internal.Logging
import org.apache.http.protocol.HTTP
import scalaj.http.Http
import scala.util.parsing.json.JSON
import org.jfarcand.wcs.WebSocket
import org.jfarcand.wcs.TextListener


/*
 * Spark Streaming Example slack receiver for slack
 */
class SlackReceiver(token: String) extends Receiver[String](StorageLevel.MEMORY_ONLY)
                    with Runnable with Logging{
  
  private val slackUrl : String = "https://slack.com/api/rtm.start"
  
  @transient
  private var thread:Thread = _
  
  
  override def onStart() :Unit = {
    thread = new Thread(this)
    thread.start()
  }
  
  override def onStop() : Unit = {
    thread.interrupt()
  }
  
  override def run() : Unit = {
    receive()
  }
  
  private def receive() : Unit = {
    val webSocket = WebSocket().open(webSocketUrl())
    webSocket.listener(new TextListener{
      override def onMessage(message: String) {
        store(message)
      }
    })
  }
  
  
  private def webSocketUrl() : String = {
    val response = Http(slackUrl).param("token", token).asString.body
    JSON.parseFull(response).get.asInstanceOf[Map[String, Any]].get("url").get.toString()
  }
  
}