
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger

object incrementalLoadInSpark extends App{
   Logger.getLogger("org").setLevel(Level.ERROR)
//requirement to do the incremental loading to a table by using Spark
//Day 1
//	id | value
//	-----------
//	1  | abc
//	2  | def
//
//Day 2
//	id | value
//	-----------
//	2  | cde
//	3  | xyz
//
//Expected result
//id | value
//-----------
//1  | abc
//2  | cde
//3  | xyz
val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "my second application")
  sparkConf.set("spark.master","local[*]")

val spark=SparkSession.builder()
.config(sparkConf)
.getOrCreate()


val list1=List((1,"abc"),(2,"def"))

import spark.implicits._
val day1=list1.toDF("id","value")

val list2=List((2,"cde"),(3,"xyz"))

val day2=list2.toDF("id","value")

val df = day1.join(day2, day1.col("id") === day2.col("id"),"full_outer")
.select(coalesce(day1.col("id"),day2.col("id")).alias("id"),coalesce(day1.col("value"),day2.col("value")).alias("value"))

df.show()
 


val newdf=day1.union(day2).dropDuplicates("id").sort("id")

newdf.show()


  
  
}