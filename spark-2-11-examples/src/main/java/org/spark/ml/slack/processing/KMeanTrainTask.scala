package org.spark.ml.slack.processing

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import java.io.File
import org.apache.spark.mllib.clustering.KMeans

object KMeanTrainTask {
  
  val numIterations = 20 

  def train(sparkContext: SparkContext, trainData:String, numClusters:Int, modelLocation: String) : KMeansModel = {
    
    
    if(new File(modelLocation).exists) removePreviousModel(modelLocation)
    
    
    val trainRdd = sparkContext.textFile(trainData)
    
    val parseRdd = trainRdd.map(Utils.featurize) 
    
    //If we have large data set to train on we'd want to call an action to trigger cache
    
    val model = KMeans.train(parseRdd, numClusters, numIterations)
    
    sparkContext.makeRDD(model.clusterCenters, numClusters).saveAsObjectFile(modelLocation)
    
//    val example = trainRdd.sample(withReplacement = false, 0.1).map(s => (s, mode))
    
//    .map(s => (s, model.predict(Utils.featurize(s)))).collect()
    
    println("Predication example")
    
//    example.foreach(println)
    
    model
  }
  
  /*
   * Remove previous model 
   */
  def removePreviousModel(path:String) ={
    
    def getRecursively(f : File): Seq[File] = f.listFiles().filter(_.isDirectory).flatMap(getRecursively) ++ f.listFiles() 
    
    getRecursively(new File(path)).foreach(f => 
      if(!f.delete())
        throw new RuntimeException("failed to be deleted "+f.getAbsolutePath)
    )
    
    new File(path).delete()
    Thread.sleep(2000)
  }
  
}