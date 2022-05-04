
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object StreamingReadingFromJson extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    
     val spark= SparkSession.builder()
  .appName("streamingWithDataframe")
  .master("local[2]")
  .config("spark.sql.shuffle.partitions",3)
  .config("spark.streaming.stopGracefullyOnShutdown","true")
  .config("spark.sql.streaming.schemaInference","true")
  .getOrCreate()
  
  //1.Read data from source
  
  val OrdersDf=spark.readStream
  .format("json")
  .option("path","myInputFolder")
  .option("maxFilesPerTrigger",1)
  .load
  
  //process
  
  OrdersDf.createOrReplaceTempView("Orders")
  
  val CompletedOrders= spark.sql("select * from Orders where order_status='COMPLETE'")
  
  //write
  
  val ordresQuery=CompletedOrders.writeStream
  .format("json")
  .outputMode("append")
  .option("path","myOutputFolder")
  .option("checkpointLocation","checkpoint-location1")
  .trigger(Trigger.ProcessingTime("30 seconds"))
  .start
  
  ordresQuery.awaitTermination()
  
  
  
  
  
  
  
  
 
    
    
    
    
    
    
    
}