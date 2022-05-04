import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object AssignMent extends App {
  
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  //problem1
 // Given an Input data set with name, age and city if age > 18 add a new column that’s populated with ‘Y’ else ‘N

  val sc=new SparkContext("local[*]","AssignMent")
  
  val input=sc.textFile("D:/TrendyTech/week9_Spark_basics/-201125-161348.dataset1")
  
 
  val Record=input.map(x=>(x.split(",")(0),x.split(",")(1).toInt,x.split(",")(2)))
  
 
  val adding_new_col=Record.map(x=>(x._1,x._2,x._3,if (x._2>18) "Y" else "N"))
  
  adding_new_col.collect.foreach(println)

  //problem 2
//Given the input file where columns are stationId, timeOfTheReading, readingType, temperatureRecorded, and few other columns.
//..We need to find the minimum temperature for each station id.  
  
  val input2=sc.textFile("D:/TrendyTech/week9_Spark_basics/tempdata-201125-161348.csv")
  
  val RecordRead=input2.map(x=>(x.split(",")(0),x.split(",")(3).toInt))
  
  val Reduceer=RecordRead.reduceByKey((x,y)=>if (x < y) x else y)
  
 // Reduceer.collect.foreach(println)
  
  lazy val output={println("Hello");1}
  println("learning Scala")
  println(output)
  
  
  
  
  
  
}