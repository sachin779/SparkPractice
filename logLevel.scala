import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source
//If we have input in terms of collection like List or array we can convert it to RDD with parallelize fumction

object logLevel extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","Loglevel")
  
  val myList=List("WARN: Tuesday 4 September 0405",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 0408")

val logsRdd=sc.parallelize(myList)
  
val intermidate=logsRdd.map(x=> {
val col=x.split(":")
val level=col(0)
(level,1)
})

val finalRdd=intermidate.reduceByKey((x,y)=>x+y)

finalRdd.collect.foreach(println)

  scala.io.StdIn.readLine()

}
  
  
