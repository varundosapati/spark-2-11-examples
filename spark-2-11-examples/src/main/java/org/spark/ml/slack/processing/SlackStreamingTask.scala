package org.spark.ml.slack.processing

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import scala.util.parsing.json.JSON

object SlackStreamingTask {
  
  
  
  def run(sparkContext:SparkContext, slackToken:String, cluster:KMeansModel, predicateOutput:String) {
    
    val ssc = new StreamingContext(sparkContext, Seconds(5))
    val dStream = ssc.receiverStream(new SlackReceiver(slackToken))
    
    //Create Stream of events from the slack .. but filter amd marshall to Json stream data 
    
    val stream = dStream
                 .filter(JSON.parseFull(_).get.asInstanceOf[Map[String, String]]("type") == "message" ) //get only messages events
                 .map(JSON.parseFull(_).get.asInstanceOf[Map[String, String]]("text") ) //extract message text from the event
                 
                 
   val kMeanStream = KMean(stream, cluster) // create k-mean model
   kMeanStream.print()
   
   if(predicateOutput != null) {
     kMeanStream.saveAsTextFiles(predicateOutput) // save the results to file if file name is specified
   }
   
    ssc.start() //run spark streaming application
    
    ssc.awaitTermination() //wait end of the application
   
   
    
  }
  
  /*
   * transform stream of strings to stream of (String, vector) pairs and set this stream as input as prediction 
   */
  
  def KMean(dStream: DStream[String], clusters:KMeansModel) : DStream[(String, Int)] = {
    dStream.map(s => (s, Utils.featurize(s))).map(p => (p._1, clusters.predict(p._2)))
  }
  
}