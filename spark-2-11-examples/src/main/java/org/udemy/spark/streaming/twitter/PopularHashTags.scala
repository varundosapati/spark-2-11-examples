package org.udemy.spark.streaming.twitter

import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization

object PopularHashTags {
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
    
    val tweets = TwitterUtils.createStream(ssc, Some(auth))
    
    //Now extract the text of each status update into Dstream using map()
    val statuses = tweets.map(status => status.getText)
    
    //Blow out each word into new DStreams 
    val tweetWords = statuses.flatMap(tweetText => tweetText.split(" "))
    
    //Now elimate anything that is not a #
    val hashTags = tweetWords.filter(word => word.startsWith("#"))
    
    //Map each hashTag with a value (key, value) pair of (hashTag, 1) so we can count them up adding the values 
    
    val hashTagKeyValues = hashTags.map(hash => (hash, 1))
    
    //Now count them up over a 5 min sliding window every one second 
    val hashTagCounts = hashTagKeyValues.reduceByKeyAndWindow((x, y) => x+y, (x, y) => x-y, Seconds(300), Seconds(1))
    
     //  You will often see this written in the following shorthand:
    //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))
    
    //Sort the result by count values 
    val sortedResults = hashTagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    
     // Print the top 10
    sortedResults.print()
    
    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
    
    
  }
  
}