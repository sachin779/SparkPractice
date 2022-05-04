import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import breeze.linalg.sum
import org.apache.spark.rdd.RDD


object WeekTwelveAssignment extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkconf=new SparkConf()
  sparkconf.set("spark.app.name","WeekTwelveAssignment")
  sparkconf.set("spark.master","local[*]")
 
  
  val spark=SparkSession.builder()
  .config(sparkconf)
  .enableHiveSupport()
  .getOrCreate()
  
  /*
  //Problem 1: Given 2 Datasets employee.json and dept.json We need to calculate the count of employees against each department.
  //Use Structured API’s.
  
  val employees=spark.read
  .format("json")
  .option("path","D:/TrendyTech/Week12_spark_api_structured/employee.json")
  .load()
  
  val dept=spark.read
  .format("json")
  .option("path","D:/TrendyTech/Week12_spark_api_structured/department.json")
  .load()
  
  //Sample output
  //depName,deptid,empcount
  //IT,11,1
  //HR,21,1
  val employeesNew=employees.withColumnRenamed("deptid","department_id")
  
  val joinDf=employeesNew.join(dept,employeesNew.col("department_id") === dept.col("deptid"),"inner")
  
  joinDf.select("deptName","id")
  .groupBy("deptName")
  .count().as("count_of_employess")  */
  //.show()

//Problem 2: Find the top movies as shown in spark practical 18 using broadcast join.
//Use Dataframes or Datasets to solve it this time.  
  
  
  /*val movies=spark.read
  .format("csv")
  .option("delimiter","\\:\\:")
  .option("path", "D:/TrendyTech/Week11_spark_api_structured/movies-201019-002101.dat")
  .load
  
    movies.show(5,false)
    
  val ratings=spark.read
  .format("csv")
  .option("delimiter","::")
  .option("path", "D:/TrendyTech/Week11_spark_api_structured/ratings-201019-002101.dat")
  .load*/
  
  val moviesRdd=spark.sparkContext.textFile("D:/TrendyTech/Week11_spark_api_structured/movies-201019-002101.dat")
  val ratingsRdd=spark.sparkContext.textFile("D:/TrendyTech/Week11_spark_api_structured/ratings-201019-002101.dat")
  
  
  trait movieratings extends Serializable

  case class movies(movieId:Int,movieName:String) extends movieratings
  case class ratings(movieIdR:Int,ratings:Float) extends movieratings
  
  
  def mapper(line:String)={
     val fields=line.split("::")
     movies(fields(0).toInt,fields(1))

  }
  
  def mapperr(line:String)={
     val fields=line.split("::")
     ratings(fields(1).toInt,fields(2).toFloat)

  }
 
  import spark.implicits._
  
  val movieDf=moviesRdd.map(mapper(_)).toDF()
  
  val ratingsDf=ratingsRdd.map(mapperr(_)).toDF()
  
 //movieDf.show(5,false)
 //ratingsDf.show(5,false)
  /*
 sample data:
 movieDf 
 +-------+----------------------------------+
|movieId|movieName                         |
+-------+----------------------------------+
|1      |Toy Story (1995)                  |
|2      |Jumanji (1995)                    |
|3      |Grumpier Old Men (1995)           |
|4      |Waiting to Exhale (1995)          |
|5      |Father of the Bride Part II (1995)|
+-------+----------------------------------
ratingsDf
+--------+-------+
|movieIdR|ratings|
+--------+-------+
|1193    |5      |
|661     |3      |
|914     |3      |
|3408    |4      |
|2355    |5      |
+--------+-------*/
 
//Problem statement:
//1.Atleast 1000 people should have rated for movie
//2.average rating should >4.5
  
ratingsDf.createOrReplaceTempView("ratingsDf")

//val df=spark.sql("select movieIdr,count(movieIdr) as cnt,avg(ratings) as avgRatings from ratingsDf group by movieIdr")
  
  
//val df1=df.filter("cnt>1000 and avgRatings>4.5")
// println(df1.count)
  
val df=  ratingsDf.groupBy("movieIdR")
  .agg(count(col("movieIdr")).as("cnt"),avg("ratings").as("avgRatings"))
  .filter("cnt>1000 and avgRatings>4.5")
  .select("movieIdr","cnt","avgRatings")
  
  

  
 //join with movie table to get the name of top movie
  
   val finaldf =df.join(broadcast(movieDf),df.col("movieIdr")===movieDf.col("movieId"),"inner")
  
   finaldf.select("movieName","avgRatings").sort("avgRatings")//.show(false)
   //scala.io.StdIn.readLine()
   
 //Problem 3: File A is a text file of size 1.2 GB in HDFS at location /loc/x. 
 //It contains match by match statistics of runs scored by all the batsman in the history of cricket.
 //File B is a text file of size 1.2 MB present in local dir /loc/y.
 //It contains list of batsman playing in cricket world cup 2019.
//Question: Find the batsman participating in 2019 who has the best average of scoring runs in his career. 
//Solve using Dataframes or Datasets.
  case class Runstatistic (MatchNumber:String,Batsman:String,Team:String,RunsScored:Float,StrikeRate:String)
  case class playerss(Batsman:String,Team:String)
   val RunStatisticsList=List(
"1 Rohit_Sharma India 200 100.2",
"1 Virat_Kohli India 100 98.02",
"1 Steven_Smith Aus 77 79.23",
"35 Clive_Lloyd WI 29 37.00",
"243 Rohit_Sharma India 23 150.00",
"243 Faf_du_Plesis SA 17 35.06"    
   )
   
   val playersList=List(
"Rohit_Sharma India",
"Steven_Smith Aus",
"Virat_Kohli India"
   )
   
   val RunStatistics= spark.sparkContext.parallelize(RunStatisticsList).map(x=>{
    val fields= x.split(" ")
    Runstatistic(fields(0),fields(1),fields(2),fields(3).toFloat,fields(4))}).toDF("MatchNumber","Batsman","Team","RunsScored","StrikeRate")
   
   val Players=spark.sparkContext.parallelize(playersList).map(x=>{
     val fields =x.split(" ")
     playerss(fields(0),fields(1))}).toDF("Batsmann","Team")
     
    
     RunStatistics.show()
   val avgCal=RunStatistics.groupBy("Batsman")
   .agg(avg(col("RunsScored")).as("avgRuns"))
   .select("Batsman","avgRuns")
   .sort("avgRuns")
   
     
   val Joindf=avgCal.join(Players,avgCal.col("Batsman")===Players.col("Batsmann"),"inner")
   
   Joindf.select("Batsmann","avgRuns")
   .orderBy(desc("avgRuns"))//.show()
   
   

}