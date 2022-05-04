import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source
//Brodcast variable example

object KeywordAmount extends App {
  
   Logger.getLogger("org").setLevel(Level.ERROR)
   val sc=new SparkContext("local[*]","AssignMent")
   
   //problem statement :Need to find total amount spend for each word 
   val input=sc.textFile("D:/TrendyTech/Week10_spark_in_depth/bigdatacampaigndata-201014-183159.csv")
   
   val inputRdd=input.map(x=>(x.split(",")(10).toFloat,x.split(",")(0)))
   //now we have ouput like below
   //(24.32,big data course)
   //Since we need to flatern out the values we have to use flatmapByValues
   
   
   val AmountPerWord=inputRdd.flatMapValues(x=> x.split(" "))
   
   val WordsAmount=AmountPerWord.map(x=>(x._2.toLowerCase(),x._1))
   //output:
   //(big,23)
   //(is,50)
   val reduceByword=WordsAmount.reduceByKey((x,y)=> x+y).sortBy(x=>x._2,false)
   
   //reduceByword.collect.foreach(println)
   
   //Here we are able to see the extra words which are not useful for eg in ,for ,because,etc
   //we have listed all these boring words in one file
   //we can use this file as a brodcast variable
   //To use it lets define a function to load this file in local..
   //.Note-we are not creating RDD we are just creating local variable to brodcast on each executor
   def loadBoringWords():Set[String] ={  
     var BoringWords:Set[String]= Set()
     
     val lines=Source.fromFile("D:/TrendyTech/Week10_spark_in_depth/boringwords-201014-183159.txt").getLines()
     
     for (line <- lines) {
       BoringWords+=line
     }
     BoringWords
   }
   
   var nameSet=sc.broadcast(loadBoringWords)
   //suppose we have "is" in BoringWords then nameset will take it 
   val BoringWordRemoved=WordsAmount.filter(x=> !nameSet.value(x._1))
   
   //now all the boring words are removed and only usefuls are there
   //we can reduce them and collect
   
   val finalrdd=BoringWordRemoved.reduceByKey((x,y)=>x+y)
   
   val sorted=finalrdd.sortBy(x=>x._2,false)

   sorted.collect.foreach(println)
   
   
   
   
   
   
  
  
  
  
}