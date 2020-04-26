package org.udemy.spark.streaming.nlp.twitter

import org.udemy.spark.streaming.nlp.twitter.util._
import twitter4j.auth.OAuthAuthorization
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Logger
import twitter4j.conf.ConfigurationBuilder
import scala.io.Source
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.spark.streaming.twitter.TwitterUtils

object StanfordTwitterNlp {
  
  /** Makes sure only ERROR messages get logged to avoid log spam**/
  def setUpLogging() = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  /** Configures Twitter Service Credentials using twitter.txt in the package workspace**/
  def setUpTwitter() : Array[String] = {
//    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) twitterCred;

    val twitterCred = new Array[String](4)
    for(line <- Source.fromFile("twitter.txt").getLines) {
      val fields = line.split(" ")
      if(fields.length == 2){
        if(fields(0).equalsIgnoreCase("consumerKey")){
          println("consumerKey"+fields(1))
          twitterCred(0) = fields(1)
        }
        
        if(fields(0).equalsIgnoreCase("consumerSecret")){
           println("consumerSecret"+fields(1))
          twitterCred(1) = fields(1)
        }
        
        if(fields(0).equalsIgnoreCase("accessToken")){
           println("accessToken"+fields(1))
          twitterCred(2) = fields(1)
        }
                
        if(fields(0).equalsIgnoreCase("accessTokenSecret")){
           println("accessTokenSecret"+fields(1))
          twitterCred(3) = fields(1)
        }
                
//        System.setProperty("../twitter4j.oauth."+fields(0), fields(1))
      }
    }
    twitterCred
  }
  
  def main(args: Array[String]):Unit = {
    //Set up twitter configuration 
    val twitterCred = setUpTwitter()
    
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = twitterCred;
    
    val filters = null
    
    val cb = new ConfigurationBuilder
    
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)
    
    val auth = new OAuthAuthorization(cb.build)
    
    //Set up streaming context named popularHashTag with 2 cores in local machine and one second batch of data 
    val ssc = new StreamingContext("local[2]", "PopularHashTag", Seconds(1))

     
    //Get rid of log spam should set up after context is set 
    setUpLogging()
    
    //Create DStream from twitter utils using streaming context 
    
    val stream = TwitterUtils.createStream(ssc, Some(auth))
 
    
//    val tweet = stream.filter(t => {
//      val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase())
//      tags.contains("#bigdata") && tags.contains("#food")
//    })
    
    stream.foreachRDD{(rdd, time) => 
        rdd.map(t => {
          Map(
            "user" -> t.getUser.getScreenName, 
            "Created-at" -> t.getCreatedAt.toInstant().toString(),
            "location" -> Option(t.getGeoLocation).map(geo => {s"${geo.getLatitude},${geo.getLongitude}"}),
             "text" -> t.getText,
             "hashTags" -> t.getHashtagEntities().map(_.getText),
             "retweet"  -> t.getRetweetCount,
//             "language" -> t.getLang,
             "sentiment" -> SentimentAnalysisUtils.detectSentiment(t.getText).toString()
          
          )
        }).saveAsTextFile("D:/ada/twitter/tweet")
      
    }
    
  ssc.start()
  
  ssc.awaitTermination()
  }
  
}