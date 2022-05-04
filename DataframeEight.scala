
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import scala.tools.nsc.typechecker.Implicits
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.expressions.Window
import java.security.Timestamp


//Pivot example

object DataframeEight extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  case class LogLevel(level:String,datetime:String)
  
  def mapper(line:String)={
    val field=line.split(",")
    
    LogLevel(field(0),field(1))
  }
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "my second application")
  sparkConf.set("spark.master","local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
  val myList=List(
"WARN,2014-7-10 23:20:22",
"FATAL,2012-8-25 05:41:46",
"WARN,2012-2-10 04:18:46",
"INFO,2013-10-2 07:34:20",
"FATAL,2012-3-10 04:06:26")
  
val rdd1=spark.sparkContext.parallelize(myList)

val rdd2=rdd1.map(mapper)

import spark.implicits._

val df1=rdd2.toDF

df1.createOrReplaceTempView("df1")

spark.sql("select * from df1").show()

spark.sql("select level,collect_list(datetime) from df1 group by 1 order by 1").show(false)

val newsome=df1.groupBy(col("level"))
.agg(collect_list(col("datetime"))).as("some")



println("new show")
newsome.show(10)

spark.sql("select level,count(datetime) from df1 group by 1 order by 1").show(false)

val df2=spark.sql("select level,date_format(datetime,'MMMM') as  month from df1")

df2.createOrReplaceTempView("df2")

spark.sql("select level,month,count(*) from df2 group by 1,2 order by 1").show(false)

//now we will try to read the big log file

val df3=spark.read
.option("header","true")
.csv("D:/TrendyTech/Week12_spark_api_structured/biglog.text")

df3.createOrReplaceTempView("df3")

val results=spark.sql("""
  select level,date_format(datetime,'MMMM') as month,date_format(datetime,'MM') as monthNumner,count(*)
  from df3 group by 1,2,3 order by 3
  """).drop(col("monthNumber"))
 println("results datafrarme")
 results.show(false)
 results.createOrReplaceTempView("results")
 //we can do a pivot
  
  val pivotResult=spark.sql("""select level,date_format(datetime,'MM') as month 
    from df3 order by month""")
  .groupBy("level").pivot("month").count().show(100,false)
 
  
  //We can do the optimization by giving pivot columns manually
  //because by default system will be calculating the distinct values of pivot columns
  
  val pivotColumns=List("January","February","March","April","May","June","July","August","September","October","November","December")
  
  val pivotOptimized=spark.sql("""select level,date_format(datetime,'MMMM') as month 
    from df3 order by month""")
  .groupBy("level").pivot("month").count().show(100,false)
 
  
  


}