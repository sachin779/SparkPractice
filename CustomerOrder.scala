import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object CustomerOrder extends App {
  
  
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc=new SparkContext("local[*]","CustomerOrder")
  val CustomerOrders=sc.textFile("D:/TrendyTech/week9_Spark_basics/customerorders-201008-180523.csv")
  
  val important_cols=CustomerOrders.map(x=>(x.split(",")(0),x.split(",")(2).toFloat))
  
  val CustomersSum=important_cols.reduceByKey((x,y) => x+y)
  
  val topCustomers=CustomersSum.sortBy(x=>x._2,false)
  
  //we can also save the result in some textfile by doing below
  //topCustomers.saveAsTextFile("D:/TrendyTech/week9_Spark_basics/")
  topCustomers.collect.foreach(println)
  
  println(topCustomers.count)
  
 
  
}