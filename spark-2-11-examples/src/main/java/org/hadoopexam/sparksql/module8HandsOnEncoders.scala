package org.hadoopexam.sparksql


/*
 * Usage - We are going to Using the encoder usage
 * args(0) - local[1]
 * args(1) - testdata\hadoopexam\sparkSql\input\module8HandsOnEncoders\
 * args(2) - testdata\hadoopexam\sparkSql\output\module8HandsOnEncoders\
 * 
 */

object module8HandsOnEncoders {

  case class Course(id: Int, name:String, fee:Int, venue:String, duration:Int)
  
  def main(args : Array[String]) : Unit = {
    
//  if(args.length > 3) {
//    println(" USAGE MASTER INPUTLOC OUTPUTLOC")
//    System.exit(1)
//  }
    
  import org.apache.spark.sql.Encoders
  
  //Create encoders as Course
  val courseEncoders = Encoders.product[Course]

  //Check whether encoder hold the schema inforamtion or not and this schema is used to encode the object as row
  courseEncoders.schema
  
  
  /*
   * ExpressionEncoders 
   */
  
  import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
  
  courseEncoders.asInstanceOf[ExpressionEncoder[Course]]
  
  //Getting serialization and de Serialization of Encoders 
  
  println(courseEncoders.asInstanceOf[ExpressionEncoder[Course]].serializer)
  
  println(courseEncoders.asInstanceOf[ExpressionEncoder[Course]].deserializer)
  
  
  val serializeRow = courseEncoders.asInstanceOf[ExpressionEncoder[Course]].toRow(Course(1, "Hadoop", 5000, "New York", 5))
  println("Serialize Row is "+serializeRow)
  
  
  import org.apache.spark.sql.catalyst.dsl.expressions._
  
  val desSerializeRow = courseEncoders.asInstanceOf[ExpressionEncoder[Course]]
                          .resolveAndBind(Seq(DslSymbol('id).int, 
                              DslSymbol('name).string , DslSymbol('fee).int, 
                              DslSymbol('venue).string, DslSymbol('duration).int)).fromRow(serializeRow)
  
                              
  println("Deserialize row is"+ desSerializeRow)              
 
  // Encoding using different mechanism like Kyro and javaSerialization
  
  Encoders.kryo[Course]
  Encoders.javaSerialization[Course]
  }
  
  
}