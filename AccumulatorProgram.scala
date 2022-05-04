import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source

object AccumulatorProgram extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
   val sc=new SparkContext("local[*]","AssignMent")
  
  val input =sc.textFile("D:/TrendyTech/Week10_spark_in_depth/samplefile-201014-183159.txt")
  
  val input_repartition=input.repartition(4)
  val myaccu=sc.longAccumulator("Blank Lines Accumulator")
  
  input_repartition.foreach(x=> if (x=="") myaccu.add(1))
  
  println(myaccu.value)
  

  
  
  
  
  
  
  
  
}