import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
//Udf function covered

object DataframeFour extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "my second application")
  sparkConf.set("spark.master","local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
  
  //reading the file
  
  val df =spark.read
  .format("csv")
  .option("inferSchema",true)
  .option("path", "D:/TrendyTech/Week12_spark_api_structured/-201025-223502.dataset1")
  .load()
  
  val df1=  df.toDF("name","age","city")
  
  case class Person(name:String,age:Int,city:String)
  import spark.implicits._  
  val DS1=df1.as[Person]
  
//Lets create a UDF
  
  def ageCheck(age:Int)={
    if (age>18) "Y" else "N"
  }  
  
//we have to register this function to use it on higher level constructs
//basically we register the function with the driver
//the driver will serialize the function and will send it to each executor
//Note:when we register the function in column expression like below it wont go to catalog
  val parseAgeFunction=udf(ageCheck(_:Int):String)
  
//whenever we want to add a new column we use .withColumn
  
val df2=df1.withColumn("adult",parseAgeFunction(column("age")))
  
df2.show()
 
 //we can do same thing with sql expressions
 //if we will register the udf using sql expressions we will 
 //be able to add the udf to catalog which will seriealize it and
 //will distribute to the executors
 //Note:In catalog sql related functions only gets added     
  spark.udf.register("parseAgeFunction",ageCheck(_:Int):String)
  val df3=df1.withColumn("adults",expr("parseAgeFunction(age)"))
  
  
 //to check which are the functions are available in spark catalog
  
  spark.catalog.listFunctions().filter(x=>x.name=="parseAgeFunction").show
  
 //we can even right a anonymous function
  
  spark.udf.register("parseAgeFunctions",(x:Int)=>{if (x>18) "Y" else "N"})
  
  val df4=df1.withColumn("adult",expr("parseAgeFunctions(age)"))
  
 //we can even create a temp view and do the same since we have added the function in catolg
  df1.createOrReplaceTempView("peopletable")
  
  spark.sql("select name,age,city,parseAgeFunctions(age) as adult from peopletable").show
  
  
}
