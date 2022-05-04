import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds



object StreamingWordCountWithWindowStateful extends App {
    
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","streaming word count with updateStateByKey")
  
  val ssc=new StreamingContext(sc,Seconds(2))
  
  val lines=ssc.socketTextStream("localhost",9998)
  
  val words=lines.flatMap(x=>x.split(" "))
  
  val wordKeys=words.map(x=>(x,1))
  //we have to save the state somewhere
  
  ssc.checkpoint("./")
  //For window stateful transaction we have to use reduceBykeyAndWindow which take 4 parametrs
  //1. summary 
  //2. revrese
  //3. window size
  //4. sliding interval
  
  val wordcounts=wordKeys.reduceByKeyAndWindow((x,y)=>x+y,(x,y)=>x-y,Seconds(10),Seconds(4))
  .filter(x=>x._2>0)
  
  wordcounts.print()
  
  ssc.start()
  ssc.awaitTermination()
  
  
   

}