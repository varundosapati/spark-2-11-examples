package org.hadoopexam.spark.streaming.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord


object KafkaProducer {
  
  def main(args : Array[String]):Unit = {
    
    
    val props = new Properties()
props.put("bootstrap.servers", "localhost:9092")
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  
  val producer = new KafkaProducer[String,String](props)
  
for(count <- 0 to 10) {
 println("Sending Message to Producer")
  producer.send(new ProducerRecord[String, String]("test-a", "title "+count.toString +" data from topic"))
  Thread.sleep(10000)
}
  

println("Message sent successfully")
producer.close()

  }
 }