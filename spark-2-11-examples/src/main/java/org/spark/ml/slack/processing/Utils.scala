package org.spark.ml.slack.processing

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

object Utils {
  
  val NUM_DIMENSION :Int = 1000
  
  val tf = new HashingTF(NUM_DIMENSION)
  
  
  /**
   * This uses min hash algorithm https://en.wikipedia.org/wiki/MinHash to transform 
   * string to vector of double, which is required for k-means
   */
  
   def featurize(s: String): Vector = {
    tf.transform(s.sliding(2).toSeq)
  }
  
  def featurize(s: RDD[Seq[String]]) :RDD[Vector] = {
    tf.transform(s)
  }
  
  
}