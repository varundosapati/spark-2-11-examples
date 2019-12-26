package org.spark.ml.slack.processing

//import com.beust.jcommander.Parameter
//import com.beust.jcommander.JCommander

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector


/*
 * local[1] input model
 * 
 * Usage - We are going to create DataFrame and DataSet from SparkContext
 * args(0) - local[1]
 * args(1) - testdata\ml\slackProcessing\input\
 * args(2) - testdata\ml\slackProcessing\output\
 */

object SlackMLApp {

  
  //    new JCommander(Config, args.toArray : _*) // TODO - need to check to see how it works 
  
  def main(args: Array[String]): Unit = {
    
    val master = args(0)
    val trainData = args(1)
    val numClusters: Int = 3
    val modelLocation = args(2)
    
    val slackToken = null;
    val predicateOutput = null;
    
    
    
    val sparkConf = new SparkConf().setMaster(master).setAppName("SlackMLApp")
    
    val sparkContext = new SparkContext(sparkConf)
    
    //Obtain existing of create a new model 
    
    val clusters :KMeansModel = 
      if(trainData != null) {
        KMeanTrainTask.train(sparkContext, trainData, numClusters, modelLocation)
      } else {
        if(modelLocation != null) {
//          KMeansModel(sparkContext.objectFile[Vector] (modelLocation).collect() )
            KMeansModel.load(sparkContext, modelLocation)
        } else {
          throw new IllegalArgumentException("Either modelLocation or trainData should be specified")
        }
      }
      
  
      if(slackToken != null) {
        SlackStreamingTask.run(sparkContext, slackToken, clusters, predicateOutput)
      }
    
  }
  
}