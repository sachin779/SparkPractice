import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object StreamingWordCountWithDataframe extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  //we have to set shuffle partition value less in streaming....in batch processing it is 200 by defaults
  val spark= SparkSession.builder()
  .appName("streamingWithDataframe")
  .master("local[2]")
  .config("spark.sql.shuffle.partitions",3)
  .config("spark.streaming.stopGracefullyOnShutdown","true")
  .getOrCreate()
  
  //read from stream
  
 val linesDf= spark.readStream
  .format("socket")
  .option("host","localhost")
  .option("port","9555")
  .load
  
  //process
  val words=linesDf.selectExpr("explode(split(value,' ')) as word")
  val countDf=words.groupBy("word").count()
  
  
  //writing to sink
  //outputmode -append,update,complete
  //There are four different trigger types for the structured streaming
  //Unspecified -In this case we are not providing any trigger configuration and the batches will be produced when new row comes from socket.
  //Time interval - We can specify after span of time for trigger event
  //One time-- has to learn
  //continuous - same as unspecified but creates batches in more faster way
  
 val wordcount= countDf.writeStream
  .format("console")
  .outputMode("complete")
  .option("checkpointlocation", "check-location1")
  .trigger(Trigger.ProcessingTime("30 seconds"))
  .start()
  
  
  wordcount.awaitTermination()
  
  
  
  
  
  
}