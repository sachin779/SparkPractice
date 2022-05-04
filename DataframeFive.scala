import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import scala.tools.nsc.typechecker.Implicits
import org.apache.spark.sql.types.DateType

//Problem statement
//Given a data
//Step 1.create a scala list
//Step 2.Create a DF from scla list and 
//col names:- orderid,orderdate,customerid,status
//Step 3.Convert orderdate field to epoch timestamp(unixtimestamp-
//number of seconds after 1st han 1970
//Step 4.Create a new column with name "newid" and make
//sure it has unique id's 
//Step 5.drop duplicateds -(orderdate,customerid)
//step 6.drop order id column


object DataframeFive extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "my second application")
  sparkConf.set("spark.master","local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
  
//Step 1.create a scala list  
  val ListOrder=List(
(1,"2013-07-25",11599,"CLOSED"),
(2,"2014-07-25",256,"PENDING_PAYMENT"),
(3,"2013-07-25",11599,"COMPLETE"),
(4,"2019-07-25",8827,"CLOSED"))
  
//step2.Create a DF from scala list and 
//col names:- orderid,orderdate,customerid,status

//Two ways to do it 
//1st. to conver the list into rdd using parallelize
  val rdd=spark.sparkContext.parallelize(ListOrder)
  import spark.implicits._
  rdd.toDF
  
//2nd. Create dataframe with the list directly
  
  val ordersDf=spark.createDataFrame(ListOrder)
  .toDF("orderid","orderdate","customerid","status")
  
  
//Step 3.Convert orderdate field to epoch timestamp(unixtimestamp-
//number of seconds after 1st an 1970

//Step 4.Create a new column with name "newid" and make
//sure it has unique id's  
////Step 5.drop duplicateds -(orderdate,customerid)
  
  val NewOrdersDf= ordersDf
  .withColumn("orderdate", unix_timestamp(col("orderdate")
  .cast(DateType)))
  .withColumn("newid",monotonically_increasing_id)
  .dropDuplicates("orderdate","customerid")
  .drop("orderid")
  .sort("orderdate")
  
  
  
  NewOrdersDf.printSchema()
  NewOrdersDf.show(false)
  
 
  
  
  spark.stop
}