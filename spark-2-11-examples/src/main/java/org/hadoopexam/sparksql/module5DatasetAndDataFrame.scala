package org.hadoopexam.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

/*
 * Usage - We are going to create DataFrame and DataSet from SparkContext
 * Note we can achieve this using SparkSession also
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\sparkSql\input\module5DatasetAndDataFrame\
 *
 */

object module5DatasetAndDataFrame {

  //Define case class with Course Details
  case class Course(id: Int, name: String, fee: Int, venue: String, duration: Int)

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("USAGE MASTER OUTPUTLOC")
      System.exit(1)
    }

    val master = args(0)
    val outputFile = args(1)

    val sparkContext = new SparkContext(master, "module5DatasetAndDataFrame", System.getenv("SPARK_HOME"))

    //Create an Rdd with 5 Course
    val inputRdd = sparkContext.parallelize(Seq(
      Course(1, "Hadoop", 6000, "Mumbai", 5),
      Course(2, "Spark", 5000, "Pune", 4), Course(3, "Python", 5000, "Hyderbad", 5), Course(4, "Scala", 4000, "Kolkata", 3), Course(5, "Hbase", 7000, "Banglore", 3)))

      inputRdd.foreach(println)
      
    val sparkSession = SparkSession.builder().master(master).appName("module5DatasetAndDataFrame").getOrCreate()

        import sparkSession.implicits._

      
    //Now Convert the above RDD into dataSet , As RDD is infer with schema is automatically converts that for dataset
    
    val inputDs = inputRdd.toDS()  
    
    println("Showing Data after for inputRdd is converted to inputDs")
    inputDs.show()
      
    //Now lets select courses conducted in mumbai and having prices more than 5000
    //NOTE : using selet which give back the DataFrame with specified columns 
    
    val filteredCoursesDf = inputDs.where('fee > 5000).where('venue==="Mumbai").select('name, 'fee, 'duration)
    
    println("Displaying course records which having fee > 5000 and venue as Mumbai ")
    filteredCoursesDf.show()
    
    //Now instead of using the SparkSQL API  we can do the same thing in SQL
   inputDs.registerTempTable("courses")
   
   val filteredSqlDS = sparkSession.sql("SELECT name, fee, duration from courses where fee > 5000 and venue == 'Mumbai'")
      
   println("Now Displaying course records running a SQL statement with condition of having fee > 5000 and venue as Mumbai")
   
   filteredSqlDS.show()

   filteredCoursesDf.write.text(outputFile)
   
  }

}