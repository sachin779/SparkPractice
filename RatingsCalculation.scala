import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object RatingsCalculation extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","RatingsCalculation")
  
  val input=sc.textFile("D:/TrendyTech/week9_Spark_basics/moviedata-201008-180523.data")
  
  val mapperInput=input.map(x=>x.split("\t")(2))
  
  val results=mapperInput.countByValue()
  results.foreach(println)
  //val ratings=mapperInput.map(x=>(x,1))
  
///  val reduceResult=ratings.reduceByKey((x,y)=>x+y)

  
  //val results=reduceResult.collect
  //results.foreach(println)
  
  
  
  
}