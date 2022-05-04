import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Logger
import org.apache.log4j.Level


object StreamingWordCount extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc =new SparkContext("local[*]","wordcount")
  
  //creating a spark streaming context
  val ssc=new StreamingContext(sc,Seconds(5))
  
  //line is a Dstream
  val lines=ssc.socketTextStream("localhost", 9995)
  
  //words is transformed Dstream
  val words=lines.flatMap(x=>x.split(" "))
  
  //assign keys to each word
  val keywords=words.map(x=>(x,1))
  
  //reducebykey and do sum of values we will get count of each word
  val result=keywords.reduceByKey((x,y)=>x+y)
  
  result.print()
  
  ssc.start()
  ssc.awaitTermination()
  
  
  
  
  
  
}