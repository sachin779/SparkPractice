import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import java.sql.Timestamp
import org.apache.log4j.Logger
import org.apache.log4j.Level


//Joins example on Dataframe

object DataframeSeven extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "my second application")
  sparkConf.set("spark.master","local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
  //WE have two datasets
  //1.Orders data with these columns-
  //order_id,order_date,order_customer_id,order_status
  //2.Customors data with these columns-
  //customer_id,customer_fname,customer_lname,customer_email,customer_password,customer_street,customer_city,customer_state,customer_zipcode

  //Step1. read both datasets
  
  val OrdersDf=spark.read
  .format("csv")
  .option("header","true")
  .option("inferSchema","true")
  .option("path","D:/TrendyTech/Week12_spark_api_structured/orders-201025-223502.csv")
  .load()
  
  val CustomerDf=spark.read
  .format("csv")
  .option("header","true")
  .option("inferSchema","true")
  .option("path","D:/TrendyTech/Week12_spark_api_structured/customers-201025-223502.csv")
  .load()
  
  //step 2.join dataframes
  //Four types of join available
  //1.inner join --will give matching records from both table
  //2.outer join --will give matching records + non matching records from left and right table
  //3.leftOuter join --will give matching records + non matching records from left table
  //4.rightOuter join --will give matching records + non matching records from left table
  
  val jointype="outer"
  val joinConditionn=OrdersDf.col("order_customer_id")===CustomerDf.col("customer_id")
  
  val joinedDf=OrdersDf.join(broadcast(CustomerDf),joinConditionn,jointype)
  
  joinedDf.show(20,false)
  
  scala.io.StdIn.readLine()  
  
}