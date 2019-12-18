package org.hadoopexam.spark.streaming

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

/*
 * Usage Explanation
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\input\module27Streamingcsv\
 * args(2) - testdata\hadoopexam\output\module27Streamingcsv\
 * 
 * NOT : For testing there needs to be new file need to be added to module27Streamingcsv
 *			 By default spark output file is overwritten once the data gets processed	
 * cd C:\Users\swathi varun\git\sparkExamples\sparkExamples> 
 * spark-submit --class "org.hadoopexam.spark.streaming.module27Streamingcsv" target\sparkExamples-0.0.1-SNAPSHOT.jar testdata\hadoopexam\input\module27Streamingcsv\ testdata\hadoopexam\output\module27Streamingcsv\
 *  
 */

object module27Streamingcsv {
  
  
  case class Trade(symbol : String, date : String, time: String, prvClose:Double, bidPrice:Double, askPrice:Double, sellprice:Double, volume:Double)extends Serializable
  
  def main(args : Array[String]) :Unit = {
   
    if(args.length < 3) {
      println("USAGE (local[1]) (inputFile) (outputFile)")
      System.exit(1)
    }
    
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)
    
    val sc = new SparkContext(master, "module27Streamingcsv", System.getenv("SPARK_HOME"))
    
    
    //Create StreamingContext (Entry point for the Streaming application) using SparkContext instance sc. We will be having batch interval as 10 seconds. 
    val streamingContext = new StreamingContext(sc, Seconds(10))
    
   /*
    *  Create input stream, which will read data from 
    *  		hdfs directory ͞/user/cloudera/stream͟
    *  		for windows machine running on windows machine inputFile testdata\hadoopexam\input\module27Streamingcsv\
    *  As soon as new file arrived in this directory, it will start creating DStream (series of rdds)
    */
    val tradeDStream = streamingContext.textFileStream(inputFile)
 
    //Print DStream content 
    tradeDStream.print()

//    tradeDStream.foreachRDD(rdd => {
//      val tradeRDD = rdd.map(_.split(",")).map(t => Trade(t(0).trim(), t(1).trim(), t(2).trim(), t(3).trim().toDouble, t(4).trim().toDouble, t(5).trim().toDouble, t(6).trim().toDouble, t(7).trim().toDouble))
//      //print first 10 records from each RDD
//      tradeRDD.take(10).foreach(println)
//      //filter all the trades , which has volume is more than 500 stocks
//      val bigTrade = tradeRDD.filter(trade => trade.volume > 500)
//      //print filtered RDD
//      bigTrade.take(2).foreach(println)
//
//      //Save filtered RDD in hdfs file system for further inspection
//      bigTrade.saveAsTextFile("/user/cloudera/stream_out")
//    }) 
    
    //As we know DStream represent collections of RDDs. Hence, process each rdd as below. 
    tradeDStream.foreachRDD( rdd => {
      
      val tradeRDD = rdd.map(_.split(",")).map(t => Trade(t(0).trim(), t(1).trim(), t(2).trim(), t(3).trim().toDouble, t(4).trim().toDouble, t(5).trim().toDouble, t(6).trim().toDouble, t(7).trim().toDouble ))
      
      //print first 10 records from each RDD
      tradeRDD.take(10).foreach(println)
      
      //filter all the trades , which has volume is more than 500 stocks 
      val bigTrade = tradeRDD.filter(trade => trade.volume > 500)
      
      //print filtered RDD
      bigTrade.take(2).foreach(println)
      
      /*
       * Save filtered RDD in hdfs file system for further inspection 
       * 	hdfs location "/user/cloudera/stream_out"
       *  for windows machine running on windows machine inputFile testdata\hadoopexam\output\module27Streamingcsv\
       */
      bigTrade.saveAsTextFile(outputFile)
      
    })
    
    //Start streaming application to receive data, and await for computation to finish. 
    streamingContext.start()
    
    streamingContext.awaitTermination()
    
    
    
  }
  
}