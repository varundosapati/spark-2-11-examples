package org.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.datastax.spark.connector.writer._

object BasicQueryCassandra {
 
  
  def main(args: Array[String]) : Unit = {
     if(args.length < 1) {
      println("Input format [local] ")
      System.exit(1)
    }
    
    val sparkMaster = args(0)
    val cassandraHost = "localhost";
      
      
//      "192.168.77.1";
      
//      "LAPTOP-3JGLOADQ";
      
//      "http://127.0.0.1:9042"
    
    
    val sparkConf = new SparkConf(true).set("spark.cassandra.connection.host", cassandraHost)
    
    val sc = new SparkContext(sparkMaster, "BasicQUeryCassandra", sparkConf)
    
    println("Without datastax")
    
    /*
     * Select the total table into RDD 
     * 
     * Assume you table test was created as CREATE TABLE javalife.kv(key text primary key, value int)
     * 
     */
    
    val data = sc.cassandraTable("javalife", "kv")
//    
//    //print some basic stats 
//    
    println("stats" +data.map(row => row.getInt("value")).stats() )
//    
    val rdd = sc.parallelize(List("moremagic", 2))

    implicit val rowWriter = SqlRowWriter.Factory
    
    val ignoreNullsWriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true)
    
    //Below statement is working
//      rdd.saveToCassandra("javalife", "kv", SomeColumns("key", "value"))
    
//    rdd.saveAsCassandraTableEx(TableDef"kv", SomeColumns("key", "value"), writeConf = ignoreNullsWriteConf)
    
//    saveToCassandra( "javalife", "kv", writeConf = ignoreNullsWriteConf)  
    
    
    
    //save from case class 
    
//    val otherRdd = sc.parallelize(List(KeyValue("magic", 0)))
    
  }
  
}