package org.hadoopexam.spark.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


/*
 * Usage Explanation
 * Below are DArguments 
 * args(0) - testdata\hadoopexam\input\module30WindowStreamingcvs\trade.csv
 * args(1) - testdata\hadoopexam\input\module30WindowStreamingcvs\company.csv
 * args(2) - testdata\hadoopexam\output\module30WindowStreamingcvs\
 * 
 * NOT : For testing there needs to be new file need to be added to module28Streamingcsv
 *			 Using sparkConf to add override existing existing outputFile	 
 * 
 * cd C:\Users\swathi varun\git\sparkExamples\sparkExamples> 
 * spark-submit --class "org.hadoopexam.spark.streaming.module30WindowStreamingcvs" target\sparkExamples-0.0.1-SNAPSHOT.jar testdata\hadoopexam\input\module30WindowStreamingcvs\trade.csv testdata\hadoopexam\input\module30WindowStreamingcvs\company.csv testdata\hadoopexam\output\module30WindowStreamingcvs\
 * TODO : Need to run this example in windows, hdfs 
 *
 * 
 */

object module30WindowStreamingcvs {
  
  
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
    
    
    val sparkConf = new SparkConf().setAppName("module30WindowStreamingcvs").set("spark.files.overwrite", "true")
    
    val sc = new SparkContext(sparkConf)
    
    val sqlContext = SQLContextSingleton.getSQLContextInstance(sc)
    import sqlContext.implicits._
    
    val sparkStreaming = new StreamingContext(sc, Seconds(batchInterval))

    val inputDStream = sparkStreaming.textFileStream(inputTradeLoc)
    
    inputDStream.print()
    
    val tradeDStream = inputDStream.window(Seconds(30), Seconds(10)).map(parseTrade)

    tradeDStream.foreachRDD({ tradeRdd => 
    
//      val sqlContext = new org.apache.spark.sql.SQLContext(tradeRdd.sparkContext)
//      import sqlContext.implicits._
      
      val tradeDF = tradeRdd.toDF()
      
      println("************Market Data **********************")
      tradeDF.show()
      tradeDF.registerTempTable("tradeTemp")
      
      val resultSet = sqlContext.sql("SELECT symbol, datevalue,  avg(volume) as avgVolume,  avg(bidprice) as avgBid , MAX(volume) maxVolume FROM tradeTemp GROUP BY symbol,datevalue")
      println("Stock Trade Statics ..")
      resultSet.show()
      
      val companyRdd = sc.textFile(inputCompanyLoc)
      companyRdd.take(5).foreach(println)
      
      val companyDF = companyRdd.toDF()
      companyDF.registerTempTable("companyTemp")
      
      val joinedResultSet = sqlContext.sql("SELECT t.symbol, c.fullname , datevalue,  avg(volume) as avgVolume,  avg(bidprice) as avgBid , MAX(volume) maxVolume, sum(volume) toatlVolume FROM tradeTemp t , companyTemp c where t.symbol = c.symbol GROUP BY t.symbol, c.fullname, datevalue")
      
      println("Stock Trade Sttaics ....")
      
      joinedResultSet.show()
      
      
    })
    
    sparkStreaming.start()
    
    sparkStreaming.awaitTermination()
  }
  
  
  object SQLContextSingleton {
    @transient private var instance: SQLContext = _

    def getSQLContextInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }
  
}