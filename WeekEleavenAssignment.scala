import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.Column
import org.apache.spark.SparkContext


object WeekEleavenAssignment extends App {
  
  //Step 1. Create a logging level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  //Step2. Create a spark session
  val sparkConf=new SparkConf
  sparkConf.set("spark.app.name", "WeekEleavenAssignment")
  sparkConf.set("spark.master","local[*]")
  sparkConf.set("spark.driver.allowMultipleContexts","true")
  
  val spark=SparkSession
  .builder()
  .config(sparkConf)
  .getOrCreate()
  
  
  // Step3: Using the standard dataframe reader API load the file and create a dataframe.
  //Note: The schema should be provided using StructType (do not use infer schema)
  //country, weeknum, numinvoices, totalquantity, invoicevalue
  val WindowDataSchema=StructType(List(
  StructField("Country",StringType),
  StructField("weeknum",IntegerType),
  StructField("numinvoices",IntegerType),
  StructField("totalquantity",IntegerType),
  StructField("invoicevalue",FloatType)
  ))
  
  val WindoDataDf=spark.read
  .format("csv")
  .schema(WindowDataSchema)
  .option("path", "D:/TrendyTech/Week11_spark_api_structured/assignment/windowdata-201021-002706.csv")
  .load()
  
  
  //Step4: Use the standard dataframe writer api to save it in parquet format. While saving make sure data 
  //is stored where we should have a folder for each country, weeknum (combination)
  //WindoDataDf.write.partitionBy("Country","weeknum").parquet("D:/TrendyTech/Week11_spark_api_structured/assignment/output/")
  
  //Step5:Also use the dataframe write api to save the data in Avro format. While saving make sure data is stored where we should have a folder for each country
  
  WindoDataDf.write
  .format("avro")
  .partitionBy("Country","weeknum")
  .option("path","D:/TrendyTech/Week11_spark_api_structured/assignment/Avrooutput/")
  .save()
  
  
  //Problem statement 2
  //Step 1.Load the data file windowdata.csv as a rdd
  case class WindowRdd(Country:String,weeknum:Int,numinvoices:Int,totalquantity:Int,invoicevalue:Float)
 
  val WindowDataRDD=spark.sparkContext.textFile("D:/TrendyTech/Week11_spark_api_structured/assignment/windowdata-201021-002706.csv")
  .map(_.split(","))
  .map(x=>WindowRdd(x(0),x(1).trim().toInt,x(2).trim().toInt,x(3).trim().toInt,x(4).toFloat))
  .repartition(8)
  
  
  val converTodf=spark.createDataFrame(WindowDataRDD)
  import spark.implicits._
  val convertTodf=WindowDataRDD.toDF
  
 // converTodf.write.json("D:/TrendyTech/Week11_spark_api_structured/assignment/problemTwoOutput/")

  


}