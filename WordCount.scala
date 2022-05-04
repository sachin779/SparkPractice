import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object WordCount extends App {
  
 Logger.getLogger("org").setLevel(Level.ERROR)
  val sc =new SparkContext("local[*]","wordcout")
  
  val input=sc.textFile("D:/TrendyTech/week9_Spark_basics/search_data.txt")
  
  
  val words=input.flatMap(x=>x.split(" "))

  val actual_words=words.map(x=> (x.toLowerCase(),1))
  
  val frequencyOfWords=actual_words.reduceByKey((x,y)=>x+y)
  
  //frequencyOfWords.collect.foreach(println)
  
  
  //To get the top words which are occured more time
  //we have to do sorting on values but there no such function
  //But we have the function sortByKey
  //First we have to shuffle the tuple ...
  //if we want to access the element 1 then we can use x._1
  //x._1 is key
  //x._2 is value
  //Other wise we can use sortBy function also 
  //sortTuple.sortBy(x=>x._2)
  val reverseTuple=frequencyOfWords.map(x=>(x._2,x._1))
  val sortTuple=reverseTuple.sortByKey(false).map(x=>(x._2,x._1))

 val result=sortTuple.collect
 for(results <- result){
   val word=results._1
   val count=results._2
   val p=println(s"$word : $count")
   
 }
  
 
  
  //scala.io.StdIn.readLine()
  
  
  
}