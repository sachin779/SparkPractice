import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


object StreamingWordCountWithStateful extends App {
  
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","streaming word count with updateStateByKey")
  
  val ssc=new StreamingContext(sc,Seconds(5))
  
  val lines=ssc.socketTextStream("localhost",9994)
  
  val words=lines.flatMap(x=>x.split(" "))
  
  val wordKeys=words.map(x=>(x,1))
  //we have to save the state somewhere
  
  ssc.checkpoint("./")
  //For stateful transaction we have to write a function which will be compatible with stateful transaction
  
  def updatefunction(newvalue:Seq[Int],previousCount:Option[Int]):Option[Int]={
    
    val count=previousCount.getOrElse(0)+newvalue.sum
    Some(count)
  }
  
  val wordcount=wordKeys.updateStateByKey(updatefunction)
  
  wordcount.print()
  ssc.start()
  
  ssc.awaitTermination()
  
  
  
  
  
  
  
  
  
  
  
  
}