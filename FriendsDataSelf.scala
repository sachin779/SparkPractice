import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger



object FriendsDataSelf extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc=new SparkContext("local[*]","FriendsDataSelf")
  
  val input=sc.textFile("D:/TrendyTech/week9_Spark_basics/friendsdata-201008-180523.csv")

  val splitrecords=input.map(x=>(x.split("::")(2).toInt,x.split("::")(3).toInt))
  //now we have the two columns as a tuple age and number of connections
  //(33,100)
  //(33,200)
  
  val reduceSetup=splitrecords.map(x=> (x._1,(x._2,1)))
  //now we have the tuple of value to do aggregation
  //(33,(100,1))
  //(33,(100,1))
  //reduceSetup.collect.foreach(println)
  val reduceIt=reduceSetup.reduceByKey((x,y)=>((x._1+y._1),x._2+y._2))
  //now we have the aggregated values
  //(33,(200,2))
  val Final=reduceIt.map(x=>(x._1,x._2._1/x._2._2))
  Final.collect.foreach(println)
   
  val final2=Final.takeSample(true,5)
  //scala.io.StdIn.readLine()
  
}  