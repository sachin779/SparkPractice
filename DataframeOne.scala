import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._

//Order.csv example of dataframe
object DataframeOne extends App{
  
  //Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf=new SparkConf()
  
  sparkConf.set("spark.app.name", "First dataframe tutorial")
  sparkConf.set("spark.master","local[2]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val orders=spark.read
  .option("header",true)
  .option("inferSchema", true)
  .csv("D:/TrendyTech/Week11_spark_api_structured/orders-201019-002101.csv")
  
  

  val a=orders.select(max(col("order_id"))).head().getInt(0)
   println(a)
  
  val customers=orders
  .repartition(4)
  .where("order_customer_id >1000")
  .select("order_id","order_customer_id")
  .groupBy("order_customer_id","order_id")
  .count()
  
  
  customers.foreach(x=>{
   println(x)
  })
  customers.show()
  
  
  
  
  Logger.getLogger(getClass.getName).info("app completed successfully")
  
  
  //scala.io.StdIn.readLine()
  
  spark.stop
}