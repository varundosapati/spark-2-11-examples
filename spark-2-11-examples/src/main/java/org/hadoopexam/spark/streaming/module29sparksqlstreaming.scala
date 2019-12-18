package org.hadoopexam.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions._

/*
 * Usage Explanation
 * Below are DArguments 
 * args(0) - testdata\hadoopexam\input\module29sparksqlstreaming\
 * trade.csv
 * args(1) - testdata\hadoopexam\input\module29sparksqlstreaming\company.csv
 * args(2) - testdata\hadoopexam\output\module29sparksqlstreaming\
 * 
 * NOT : For testing there needs to be new file need to be added to module28Streamingcsv
 *			 Using sparkConf to add override existing existing outputFile	 
 * 
 * cd C:\Users\swathi varun\git\spark-2-11-examples\spark-2-11-examples\> 
 * spark-submit --class "org.hadoopexam.spark.streaming.module29sparksqlstreaming" target\spark-2-11-examples-0.0.1-SNAPSHOT.jar testdata\hadoopexam\input\module29sparksqlstreaming\ testdata\hadoopexam\input\module29sparksqlstreaming\company.csv testdata\hadoopexam\output\module29sparksqlstreaming\
 * TODO : Need to run this example in windows, hdfs 
 */



object module29sparksqlstreaming {
  
  /*
   * Trade Case class
   */

  case class Trade(symbol :String, datavlaue:String, timevalue:String, prvclose:Double,bidprice:Double, askprice :Double, sellprice:Double, volume:Double  )extends Serializable
  
  
  /*
   * Organization case
   */
  case class Company(symbol :String, fullname:String) extends Serializable
  
  
  def parseTrade(line : String) : Trade = {
    val t = line.split(",")
    Trade(t(0).trim(), t(1).trim(), t(2).trim(), t(3).trim().toDouble, t(4).trim().toDouble, t(5).trim().toDouble, t(6).trim().toDouble, t(7).trim().toDouble)
  }
  
  def parseCompany(line : String) : Company = {
    val c = line.split(",")
    Company(c(0).trim(), c(1).trim())
  }
  
  val batchInterval  =10
  
  def main(args : Array[String]) :Unit ={
    
    if(args.length < 3){
      println("USAGE INPUTLOCN INPUTFILE OUTPUTLOCN")
      System.exit(1)
    }
    
    val inputTradeLoc = args(0)
    
    val inputCompanyLoc = args(1)
    
    val outputLoc = args(1)
    
    
    val sparkConf = new SparkConf().setAppName("module29sparksqlstreaming").set("spark.files.overwrite", "true")
    
    val sc = new SparkContext(sparkConf)
    
    val sqlContext = SQLContextSingleton.getSQLContextInstance(sc)
    import sqlContext.implicits._
    
    val streamingContext = new StreamingContext(sc, Seconds(batchInterval))
    
    val inputDStream = streamingContext.textFileStream(inputTradeLoc)

    inputDStream.print()
    
    val tradeDStream  = inputDStream.map(parseTrade)
    
    tradeDStream.foreachRDD(tradeRdd => {
      
      tradeRdd.take(10).foreach(println)
      
      if(!tradeRdd.partitions.isEmpty) {
        
//        val sqlContext = new org.apache.spark.sql.SQLContext(tradeRdd.sparkContext)
//        import sqlContext.implicits._
        
        val tradeDF = tradeRdd.toDF()
        println("********************Market Data*******************")
        tradeDF.show()
        
        tradeDF.registerTempTable("tradeTemp")
        
        val resultSet = sqlContext.sql("SELECT symbol, datevalue, avg(volume) as avgVolume, avg(bidprice) as avgbid, Max(volume) maxVolume FROM tradeTemp GROUP BY symbol, datevalue ")
        println("Stock trade statics ....")
        
        resultSet.show()
        
        val companyRdd = sc.textFile(inputCompanyLoc)
        companyRdd.take(10).foreach(println)
        
        val companyDf = companyRdd.toDF()
        companyDf.registerTempTable("companyTemp")

        val joinedResultSet = sqlContext.sql("SELECT t.symbol, c.fullname, datevalue, avg(volume) as avgVolume, avg(bidprice) as avgbid, Max(volume) maxVolume  FROM tradeTemp t, companyTemp c where t.symbol = c.symbol GROUP BY t.symbol, c.fullname, datevalue")
      
        println("STOCK TRADE STATICS ..")
        
        joinedResultSet.show()
      }
      
      
    })
    
    streamingContext.start()
    
    streamingContext.awaitTermination()
    
  }
  
  object SQLContextSingleton {
    
    @transient private var instance : SQLContext = _
    
    def getSQLContextInstance(sparkContext: SparkContext) : SQLContext = {
      if(instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
    
  }
  
  
  
}