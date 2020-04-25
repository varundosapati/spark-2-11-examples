package org.hadoopexam.spark.streaming.kafka

import _root_.kafka.serializer.DefaultDecoder
import _root_.kafka.serializer.StringDecoder
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object BasicKafkaTestTopic {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BasicKafkaTestTopic").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    // prevent INFO logging from pollution output
    sc.setLogLevel("ERROR")

    // creating the StreamingContext with 5 seconds interval
    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "org.hadoopexam.spark.streaming.kafka",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("test", "test-a")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
         
    stream.map(record=>(record.value().toString)).print
    
    ssc.start()
    
    ssc.awaitTermination()
  }
}

//    stream.map(record => (record.key, record.value))

    //val kafkaConf = Map(
    //    "metadata.broker.list" -> "localhost:9092",
    //    "zookeeper.connect" -> "localhost:2181",
    //    "group.id" -> "kafka-streaming-example",
    //    "zookeeper.connection.timeout.ms" -> "1000"
    //)
    //
    //val lines = KafkaUtils.create
    //createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
    //    ssc,
    //    kafkaConf,
    //    Map("test" -> 1),   // subscripe to topic and partition 1
    //    StorageLevel.MEMORY_ONLY
    //)
    //
    //val words = lines.flatMap{ case(x, y) => y.split(" ")}
    //
    //words.print()