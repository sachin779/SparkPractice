import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

//Dealing with unstructured data with spark
//We have to create structured form of this unstructured data using low level constructs on RDD
//then we can convert it to Dataset or Dataframe to do the high level constructs like select,where,groupby


object DataframeThree extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //First we created a regular exp variable based on that we will be able to split the data
  val myregex="""^(\S+) (\S+)\t(\S+)\,(\S+)"""r
  
  //Then we can create a case class to get the data in correct types
  
  case class Order(order_id:Int,customer_id:Int,order_status:String)
  
  //After reading data we get the line of string and to convert it to structured data as we want 
  //we can do it with map which is a higher order function and takes other functions as a param
  
  def parser (line:String)={
    line match{
      case myregex(order_id,date,customer_id,order_Status)=>
        Order(order_id.toInt,customer_id.toInt,order_Status)
    }
  
  }
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "my second application")
  sparkConf.set("spark.master","local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
  val LinesRdd=spark.sparkContext.textFile("D:/TrendyTech/Week12_spark_api_structured/new_orders.csv")

  
  val rdd= LinesRdd.map(parser)
  
  import spark.implicits._
  val convertDs=rdd.toDS().cache()
  
  convertDs.select("order_id").show
  
  convertDs.groupBy("order_status").count.show()
  
  convertDs.filter(x=>x.customer_id >2)
  
  
  //lets try with dataframe
  
  val LinesDf=spark.read.text("D:/TrendyTech/Week12_spark_api_structured/new_orders.csv")
  
 val col1=LinesDf
 .withColumn("order_no", regexp_extract(col("value"),"""^(\S+) (\S+)\t(\S+)\,(\S+)""",1))
 .withColumn("customer_id", regexp_extract(col("value"),"""^(\S+) (\S+)\t(\S+)\,(\S+)""",3))
 .withColumn("order_status", regexp_extract(col("value"),"""^(\S+) (\S+)\t(\S+)\,(\S+)""",4))
 .drop("value")
 
 
import spark.implicits._
 
 val dfnew=col1.as[Order]
 
 
 
  spark.stop
  
}