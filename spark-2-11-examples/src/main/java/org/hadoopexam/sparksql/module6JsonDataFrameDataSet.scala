package org.hadoopexam.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import org.apache.spark.sql.functions.col

import org.apache.log4j._
import org.apache.spark.sql.SaveMode

/*
 * Usage - We are going to create DataFrame and DataSet from SparkContext
 * args(0) - local[1]
 * args(1) - testdata/hadoopexam/sparkSql/input/module6JsonDataFrameDataSet/
 * args(2) - testdata\hadoopexam\sparkSql\output\module6JsonDataFrameDataSet\
 */
object module6JsonDataFrameDataSet {
 Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args : Array[String]) : Unit = {
    if(args.length < 3){
      println("USAGE MASTER INPUTFILELOC OUTPUTFILELOC")
      System.exit(1)
    }
    
    val master = args(0)
    val inputLoc = args(1)
    val outputLoc = args(2)
    
    val sparkConf = new SparkConf().setMaster(master).setAppName("module6JsonDataFrameDataSet")
    
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    
    /*
     * Load csv using sparkSession sql , This also shows that SparkSql also support hive query lanaguage
     */
    
    sparkSession.sql("CREATE OR REPLACE TEMPORARY VIEW TRAININGVIEW USING csv OPTIONS "+
      "('path'='"+ inputLoc+"Training.csv', 'header'='true')")
     
      // Select all the records from HiveQL Syntax
     println("Selecting the reocrds using HiveQL syntax") 
     sparkSession.sql("FROM TRAININGVIEW").show 
    
     println("Display schema detail for TrainingView")
    sparkSession.sql("desc EXTENDED TRAININGVIEW").show()
    
    
    /*
     * Loading JSON data using sparkSession with selecting specific fields
     * NOTE: If format is not defined spark assumes it is parquet file by default
     */
     
    println("Selecting Json data using SparkSession and selecting records which are having fee > 5000")
    
    /*
     * TODO: was unable to use .where($"fee" > 5000) and used .where(col("fee") > 5000) by importing col
     */
    sparkSession.read.format("json").load(inputLoc+"data.json").select("name", "fee", "venue").toDF().where(col("fee") > 5000).show()
    
    /*
     * Explicitly specifying schema when loading data from json data 
     */
    import org.apache.spark.sql.types._
    
    val schemaType = StructType(
          StructField("id", LongType, nullable = false)  ::  
          StructField("name", StringType, nullable = false) ::
          StructField("fee", DoubleType, nullable = false) ::
          StructField("venue", StringType, nullable = false) ::
          StructField("duration", LongType, nullable = false) ::
          Nil
    )
    
    //Now we can reading the json data by explicityly specifying schema 
    
    sparkSession.read.format("json").schema(schemaType).load(inputLoc+"data.json").where('fee > 5000).show()
    
    /*
     * Loading data using json data and using if condition in sql statement
     */
    
  val jsonData =  sparkSession.read.format("json").load(inputLoc+"data.json")
  
  jsonData.show()
  
  jsonData.createOrReplaceTempView("JsonData")
  
   val filteredDf =  sparkSession.sql("select name, IF(fee > 5000 , fee, 5000) FROM (SELECT name, fee from JsonData) temp")
   
   filteredDf.show()
   
   filteredDf
     .write
     .mode(SaveMode.Overwrite)
     .json(outputLoc+"data")
    
    
  }
  
  
}