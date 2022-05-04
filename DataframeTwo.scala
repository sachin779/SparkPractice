import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import java.sql.Timestamp
import org.apache.spark.sql.SaveMode

//1.Difference between dataframe and dataset elaborated  

//2.Options to define a schema 

//3.Save the data as a table 
case class OrdersData (order_id:Int,order_date:Timestamp,order_customer_id:Int,order_status:String)
object DataframeTwo extends App {
  
  
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "my second application")
  sparkConf.set("spark.master","local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
  
   val orders: Dataset[Row]=spark.read
  .option("header",true)
  .option("inferSchema", true)
  .csv("D:/TrendyTech/Week11_spark_api_structured/orders-201019-002101.csv")
  
  //when we give orderids instead of order_id..since this is a dataframe it wont give compile time error
  //it will give error at runtime
  
  orders.filter("order_id< 10")
  
  //Now let us convert our dataframe to a dataset
  //This is where we require a case class.
  //create a case class and create dataset[OrdersData]...OrdersData will be a case class
  //To convert a dataframe in dataset or voice versa we need to 
  //import implicits after creating as spark session
  import spark.implicits._
  
  val ordersDs=orders.as[OrdersData]
  
  //we can get the error at compile time when we use the wrong column name
  
  ordersDs.filter(x=>x.order_id>10)

  
  
  
  
  //Options to define a schema starts here
  //1.programatically
  val OrderSchema=StructType(List(
  StructField("orderid",IntegerType,true),
  StructField("orderdate",TimestampType,true),
  StructField("customerid",IntegerType,true),
  StructField("status",StringType,true)//third field in this is a nullable true or false...we can set this to false if we are not expecting nulls for particular column
  ))
  
  
   val ordersDf=spark.read
  .format("csv")
  .option("header",true)
  .schema(OrderSchema)
  .option("path","D:/TrendyTech/Week11_spark_api_structured/orders-201019-002101.csv")
  .load()
  
  ordersDf.printSchema
  ordersDf.show(false)
  
  
  
  
  //3 Save the dataframe as table
  //we need to add a jar for hive support
  //we have to enable the hive support by  adding .enableHiveSupport() property while
  //spark config
  //To create the new database we can use below code
  spark.sql("create database if not exists retail")
  //To write a data into table format
  ordersDf.write
  .format("csv")
  .bucketBy(4,"customerid")
  .sortBy("customerid")
  .mode(SaveMode.Overwrite)
  .saveAsTable("retail.Orders")
  
  //To get the list of tables in db
  
  spark.catalog.listTables("retail").show()
  
  

}