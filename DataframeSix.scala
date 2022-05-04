import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import scala.tools.nsc.typechecker.Implicits
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.expressions.Window

//Aggregation transformation example
//Three types
//1.Simple aggregation Example

//Problem Statement
//Step 1.Load the file and create a dataframe.I should do it
//using standard dataframe reder api.
//calculate below using column object expression
//totalNumbersOfRows,totalQuantity,avgUnitPrice,numberOfUniqueInvoices
//Step 2.Calculate this using column object expression
//Step 3.Do the same with string expression or column expression.
//Step 4.Do the same with spark sql


object DataframeSix extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "my second application")
  sparkConf.set("spark.master","local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
//1.Simple aggregation
//Step 1.Load the file and create a dataframe.I should do it
//using standard dataframe reder api.
  val invoiceDf=spark.read
  .format("csv")
  .option("header","true")
  .option("inferSchema","true")
  .option("path","D:/TrendyTech/Week12_spark_api_structured/order_data-201025-223502.csv")
  .load()
  
//calculate below using column object expression
//totalNumbersOfRows,totalQuantity,avgUnitPrice,numberOfUniqureInvoices
//Step 2.Calculate this using column object expression
    
   invoiceDf.select(
   count("*").as("row_count"),
   sum("Quantity").as("totalQuantity"),
   avg("UnitPrice").as("avgUnitPrice"),
   countDistinct("InvoiceNo").as("CountDistinct")
   )
  
//Step 3.Do the same with string expression or column expression.

   invoiceDf.selectExpr(
       "count(*) as RowCount",
       "sum(Quantity) as totalQuantity",
       "avg(UnitPrice) as avgUnitPrice",
       "count(Distinct(InvoiceNo)) as CountDistinct"
   )//.show()
   

//Step 4.Do the same with spark sql
   invoiceDf.createOrReplaceTempView("invoiceDf")
   
   spark.sql("select count(*),sum(Quantity) as totalQuantity,avg(UnitPrice) as avgUnitPrice,count(Distinct(InvoiceNo)) as CountDistinct from invoiceDf")//.show()
   

///2.Group by aggregation
//Problem statement
//Step1: Group the data based on Country and Invoice Number
//Step2: I want total quantity for each group, sum of invoice value
//Step3: Do it using column object
//Step4: Do it using string expression
//Step5: Do it using spark sql.
   

   invoiceDf.groupBy("Country","InvoiceNo")
   .agg(sum("Quantity").as("SumOfQuantity"),
        sum(expr("Quantity * UnitPrice")).as("SumOFInvoice")
   )//.show(false)
   
   //Step4: Do it using string expression
   invoiceDf.groupBy("Country","InvoiceNo")
   .agg(expr("sum(Quantity) as SumOfQuantity"),
       expr("sum(Quantity * UnitPrice) as SumOFInvoice")).show(false)
       
   //Step5: Do it using spark sql.
    
     invoiceDf.createOrReplaceTempView("sales")
     
    spark.sql("""select Country,InvoiceNo,
      sum(Quantity) as SumOfQuantity,sum(Quantity * UnitPrice) as SumOFInvoice 
      from sales group by Country,InvoiceNo     
    """)
    
    
//3.Window aggregation
    
//Step 1.reading the data
      val invoiceNewDf=spark.read
  .format("csv")
  //.option("header","true")
  .option("inferSchema","true")
  .option("path","D:/TrendyTech/Week12_spark_api_structured/windowdata-201025-223502.csv")
  .load()
  
  //For Windowing we should write below three properties
  //1.partition column/group by column -here Country
  //2.Order by column   -here weeknum
  //3.the window size - from 1st row to the current row
  //step2.define the window based on these three parameters
  val myWindow=Window.partitionBy("country")
  .orderBy("weeknum")
  .rowsBetween(Window.unboundedPreceding,Window.currentRow)
  
  //Step 3.create a new column with aggregation
  
  val newDf=invoiceNewDf
  .toDF("country","weeknum","numinvoices","totalquantity","invoicevalue")
  .withColumn("RunningTotal", sum("invoicevalue").over(myWindow))
  
newDf.show()
   
}