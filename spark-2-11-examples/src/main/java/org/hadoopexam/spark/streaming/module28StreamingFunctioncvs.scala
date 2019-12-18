package org.hadoopexam.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming._
/*
 * Usage Explanation
 * args(0) - testdata\hadoopexam\input\module28StreamingFunctioncvs\
 * args(1) - testdata\hadoopexam\output\module28StreamingFunctioncvs\
 * 
 * NOT : For testing there needs to be new file need to be added to module28Streamingcsv
 *			 Using sparkConf to add override existing existing outputFile	 
 */


object module28StreamingFunctioncvs {
  
  
  /*
   * Create a case Class of trade
   */
   case class Trade(symbol : String, date : String, time: String, prvClose:Double, bidPrice:Double, askPrice:Double, sellprice:Double, volume:Double)extends Serializable
  
   /*
    * Create a function which process each line to Trade Object
    */
   
   def processTrade(line :String) : Trade = {
     val t  = line.split(",")
     Trade(t(0).trim(), t(1).trim(), t(2).trim(), t(3).trim().toDouble, t(4).trim().toDouble, t(5).trim().toDouble, t(6).trim().toDouble, t(7).trim().toDouble )
   }
  
   val batchInterval = 10
   
  def main(args : Array[String]) : Unit = {
    
    if(args.length < 2)
    println("USAGE INPUTFILE OUTPUTFILE")
    System.exit(1)

    //    val master = args(0)
    val inputLocation = args(0)
    val outputLocation = args(1)
  
    // Create sparkConf 
    
    //If we do not want to override data we can set spark.files.overwrite as false
    val sparkConf = new SparkConf().setAppName("module28StreamingFunctioncvs").set("spark.files.overwrite", "true")
    
    //Getting sparkContext based on sparkConf
    val sc = new SparkContext(sparkConf)
    
    //Getting streaming Context based on sparkContext
    val streamingContext = new StreamingContext(sc, Seconds(batchInterval))
    
    /*
     * Give input stream location 
     * 
     * for cloudera hdfs /user/cloudera/stream
     * for windows file location - testdata\hadoopexam\input\module28StreamingFunctioncvs\
     */
//    C:\Users\swathi varun\git\sparkExamples\sparkExamples\testdata\hadoopexam\input\module28StreamingFunctioncvs\
//    val inputDStream = streamingContext.textFileStream(inputLocation)
    
        val inputDStream = streamingContext.textFileStream(inputLocation)
    
    inputDStream.print()
    
    println("Happening module D Stremaing functions")
    
    val tradeDStream = inputDStream.map(processTrade)
    
    /*
     * Loop through dstream and get each rdd in stream and get the trade which are greater than 500 volume
     */
    tradeDStream.foreachRDD(tradeRdd => {
      
      tradeRdd.take(10).foreach(println)
      
      if(!tradeRdd.partitions.isEmpty) {
        val bigtradeRdd = tradeRdd.filter(trade => trade.volume > 500)
        
        bigtradeRdd.take(2).foreach(println)
        
        bigtradeRdd.saveAsTextFile(outputLocation)
      }
      
    })
    
    streamingContext.start()
    
    streamingContext.awaitTermination()
    
  
  }
  
  
}