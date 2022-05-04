import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._

object StreamingWithTumblingWindow  extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
   
  val spark=SparkSession.builder()
  .master("local[*]")
  .appName("Streaming with Tumbling Window")
  .config("spark.streaming.stopGracefullyOnShutdown","true")
  .config("spark.sql.shuffle.partitions",3)
  .getOrCreate()
  
  //read the data from socket
  val ordersDf=spark.readStream
  .format("socket")
  .option("host","localhost")
  .option("port",12345)
  .load()
  
  //ordersDf.printSchema
  
  val SchemaOrder=StructType(List(
  StructField("order_id",IntegerType),
  StructField("order_date",StringType),
  StructField("order_customer_id",IntegerType),
  StructField("order_status",StringType),
  StructField("amount",IntegerType)
  ))
  
  
  //process
  
  val value_df= ordersDf.select(from_json(col("value"),SchemaOrder).alias("value"))
  
  //value_df.printSchema()
 //this will give all the columns under value struct like below
//  root
// |-- jsontostructs(value): struct (nullable = true)
//|    |-- order_id: integer (nullable = true)
//|    |-- order_date: string (nullable = true)
//|    |-- order_customer_id: integer (nullable = true)
//|    |-- order_status: string (nullable = true)
//|    |-- amount: integer (nullable = true)

  //to get the columns only as a table structure in dataframe we need to have one more transformation
  
  val normal_df=value_df.select("value.order_id", "value.order_date","value.order_customer_id","value.order_status","value.amount")
  
  //normal_df.printSchema()
//This will give dataframe with actual columns
//|    |-- order_id: integer (nullable = true)
//|    |-- order_date: string (nullable = true)
//|    |-- order_customer_id: integer (nullable = true)
//|    |-- order_status: string (nullable = true)
//|    |-- amount: integer (nullable = true)

  
  //now tumbling window
  //we can add watermark config while windowing for not to consider the records after particular period of time(in this case 30 minutes)
  val windowAggDf=normal_df
  //.withWatermark("order_date","30 minutes")
  .groupBy(window(col("order_date"),"15 minutes"))
  .agg(sum("amount")
  .alias("totalInvoice"))
  
//  windowAggDf.printSchema()
  //it will give same struct schema just like above
  
  //to sort it out we have to create one more df
  
  val outputDf=windowAggDf.select("window.start","window.end","totalInvoice")
  
  //write to sync
  
  val ordersQuery=outputDf.writeStream
  .format("console")
  .outputMode("update")
  .option("checkpointlocation","checkpoint-location1")
  .trigger(Trigger.ProcessingTime("15 seconds"))
  .start()
  
  
  
  ordersQuery.awaitTermination()
  
  
  
  
  
  
  
 
  
  
  
  
  
}