

import org.apache._
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{json_tuple,to_json,from_json}
import org.apache.spark.sql.functions.lpad
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.expressions.Window
import java.sql.Time
import java.text.SimpleDateFormat
import org.apache.log4j.Level
import org.apache.log4j.Logger
//import org.spark_project.io.netty.handler.codec.http2.StreamBufferingEncoder.DataFrame



object SparkInterviewQuestions extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf=new SparkConf()
  
  sparkConf
  .set("spark.app.name", "SparkInterviewQuestions")
  .set("spark.master","local[*]")
  .set("spark.sql.crossJoin.enabled","true")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
  
//Question 1.How to find max and min from multiple columns for the same record or row in spark dataframe 
  
  
  val list_date=List(
  Row("Babu",20,33,60,44),
  Row("Raja",58,33,78,83),
  Row("Mani",75,81,56,67),
  Row("Kalam",100,100,93,87)
  )
  
 // case class schema_list (name:String,Term1:Int,Term2:Int,Term3:Int,Term4:Int)
  val schema1=StructType(List(
  StructField("name",StringType,true),
  StructField("Term1",IntegerType,true),
  StructField("Term2",IntegerType,true),
  StructField("Term3",IntegerType,true),
  StructField("Term4",IntegerType,true) ))
    
    
  
  val data_1=spark.sparkContext.parallelize(list_date)
  
  val data=spark.createDataFrame(data_1,schema1)//createDataFrame(list_date,list_Schema)
  
  val GetMaxMinForEachRow=data.withColumn("Maxx",greatest("Term1","Term2","Term3","Term4"))
  .withColumn("Minn",least("Term1","Term2","Term3","Term4"))
  
  //GetMaxMinForEachRow.show(false)
  
//Question 2. Get the total time travelled from one station to another station by bus.
  
  val bus_list=List(
  Row(1,"Station1","4:20 AM"), 
  Row(1,"Station2","5:30 AM"),
  Row(1,"Station3","7:30 AM"),
  Row(2,"Station2","5:50 AM"),
  Row(2,"Station4","7:30 AM"),
  Row(2,"Station5","11:30 AM"),
  Row(2,"Station6","1:30 PM")
  )
  
  val bus_schema=StructType(List(
  StructField("Bus_Id",IntegerType,true),
  StructField("Station",StringType,true),
  StructField("Time",StringType,true)
  ))
  
  val dataRdd=spark.sparkContext.parallelize(bus_list)
  val df=spark.createDataFrame(dataRdd,bus_schema)
  
  //spark.sql.crossJoin.enabled=true
  //We are trying cross join to get start and end time from station to station
  df.join(df.as("df2"),"Bus_Id")//.show(100)
  //Lets create a window of bus and time
  //val windows=Window.partitionBy("Bus_ID").orderBy(to_timestamp(col("Time"),"hh mm a").asc())
  
  //To understand the timestamp writing below lines
 /*  df.select(unix_timestamp(col("Time"), "hh:mm a")).show()
   df.select(from_unixtime(unix_timestamp(col("Time"), "hh:mm a"),"hh:mm a")).show()
   df.select(to_timestamp(col("Time"),"hh:mm a")).show()*/
   
  val windows=Window.partitionBy("Bus_ID").orderBy(to_timestamp(col("Time"),"hh:mm a"))
  val dif_wind=df.withColumn("row_num",row_number().over(windows))
  println("print dif wind")
  //dif_wind.printSchema
 //dif_wind.show()
  
 
  
  
  //Now apply cross join
  val df_out=dif_wind.join(dif_wind.alias("df2"),
  (dif_wind.col("row_num")<col("df2.row_num") 
  &&
  dif_wind.col("Bus_Id")===col("df2.Bus_Id")))
  .select(dif_wind.col("Bus_Id"),
          dif_wind.col("Station").alias("source_point"),
          dif_wind.col("Time").alias("Source_Time"),
          col("df2.Station").alias("dest_point"),
          col("df2.Time").alias("Dest_Time"))
          
  //df_out.show()
  
  val df_final=df_out.withColumn("Travel_time",(to_timestamp(col("Dest_Time"),"hh:mm a").cast("long")
      -to_timestamp(col("Source_Time"),"hh:mm a").cast("long"))/60).drop("Source_Time","Dest_Time").orderBy("source_point","dest_point")
  //df_final.show()
      
      
      
//Question 3. Consider we have a input Spark dataframe as shown in the below with the couple of column Name and Score.
      
 /*Our requirement is to,
Add Leading Zero to the column Score and make it column of three digit as shown in the below output Spark dataframe. 
 for example, convert 89 to 089 and convert 8 into 008*/
      
val Q3Data=List(
Row("babu",20),
Row("Raja",8),
Row("Mani",75),
Row("Kalam",100),
Row("Zoin",7)
)


val q3Schema=StructType(List(
    StructField("name",StringType),
    StructField("score",IntegerType)))

val q3rdd=spark.sparkContext.parallelize(Q3Data)

import spark.implicits._

val q3df=spark.createDataFrame(q3rdd, q3Schema)

//q3df.show

//Method 1.Using Lpad

val lpaddf=q3df.withColumn("lpaded_col", lpad(col("score"),3,"0"))

//lpaddf.show(false)

//Method 2.Using Format string

val FormatStringDf=q3df.withColumn("Fpaded_col", format_string("%03d",col("score")))
.withColumn("Fpaded_col2", format_string("%s#%03d",col("name"),col("score")))

//FormatStringDf.show()
      
      
//Question 4.Consider we are having 5 folders in same path with multiple csv files inside it .
//What is a optimized way to read the csv files inside 3 folders out of 5.

//Method1. create a list of folders which we need to read and pass to the read function
//This approch will work out with the spark version 3.03 currently spark version in 2.7 so it will not work
//val listOfPath=
 // List("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/Data1/*.csv","D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/Data2/*.csv","D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/Data3/*.csv")
      
//val q4df=spark.read.csv(s"$listOfPath")
//val newread=spark.read.csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/Data1/*.csv")
//    q4df.show  
      
 //Method2. Regular expression

val q4df=spark.read.option("header", "true").csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/Data[1-3]*/*.csv")
  //q4df.show
  
 //Method3.Using curly braces 
  
val q4df2=spark.read.option("header", "true")
.csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/Data{1,2,4}*/*.csv")
//q4df2.show
 

//Question 5.Date format question
//From the given csv file calculate expiry date by adding rechargedate column with remaining days column.

val q5df=spark.read.option("header","true").option("inferSchema","true")
.csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/date_duration/date_duration.csv")
//q5df.printSchema
//q5df.show

val q5df2=q5df.withColumn("recharge_date_format", to_date(col("Rechargedate").cast("string"),"yyyymmdd"))
//q5df2.show

q5df2.selectExpr("*","date_format(date_add(recharge_date_format,Remaining_days),'yyyyMMdd') as expiray_date")//.show


//Question 6. Multidelimeter read in spark dataframe
//multidelimeter is taken care in spark 3.0 onwards so not doing this one.
val q6df=spark.read
.text("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/multidelimeter/multidelimeter.txt")
  
//q6df.show(false)

val headerr= q6df.first()(0).toString()
val something="Name~|Age"
//val Schema=headerr.split("""\~\|""").toList

//q6df.printSchema

//q6df.filter(col("value") != something).show
 
 
//Question 7.Merging dataframes into one 
//Consider we are having two files - File 1 and File 2 with different schema, how will you merge both file into a single dataframe

val qs7df1=spark.read.option("header","true").option("delimiter","|").csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/mergeTwoDfs/File1.txt")
val qs7df2=spark.read.option("header","true").option("delimiter","|").csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/mergeTwoDfs/File2.txt")

//qs7df1.show()
//qs7df2.show()
//Mehtod 1.using withcolumn we will add one column in df1 to make the number of columns same for union

val qs7df1b=qs7df1.withColumn("Gender", lit("null"))


val qs7union1=qs7df2.union(qs7df1b)

//qs7union1.show()



//Method 2.Define a schema for both the dataframes and then union

val Schemaq7=StructType(List(
 StructField("Name",StringType,true),
 StructField("Age",IntegerType,true),
 StructField("Gender",StringType,true)))

 //read again using schema
val qs7dfm1=spark.read.option("header","true").option("delimiter","|").schema(Schemaq7)
.csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/mergeTwoDfs/File1.txt")
val qs7dfm2=spark.read.option("header","true").option("delimiter","|").schema(Schemaq7)
.csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/mergeTwoDfs/File2.txt")

val qs7union2=qs7dfm1.union(qs7dfm2)

//Method 3.Do the outer join


val qs7joindf=qs7df1.join(qs7df2,Seq("Name","Age"),"outer")


//qs7joindf.show()

 

//Method 4.Automated Approch

val merged_cols=qs7df1.columns.toSet ++ qs7df2.columns.toSet

def get_new_cols(Existing_cols:Set[String],merged_col:Set[String])={
 merged_col.map(x=> x match {
   case x if Existing_cols.contains(x) => col(x)
   case _ => lit("null").as(x)  
 }    
 )  
}
  
 // val df1_new=qs7df1.select(get_new_cols(qs7df1.columns.toSet,merged_cols):_*)
  
  val new_df1=qs7df1.select(get_new_cols(qs7df1.columns.toSet, merged_cols).toSeq:_*)//.show()
  val new_df2=qs7df2.select(get_new_cols(qs7df2.columns.toSet, merged_cols).toSeq:_*)//.show()

  val qs7uniondf=new_df1.union(new_df2)
 // qs7uniondf.show()
  
//Question 8.Consider a file with a column named Education,which has array of elements.(Expload)
//Convert each element in the array to record using spark dataframe
  //Given data
  //Name|Age|Education
  //Azar|25|MBA,BE,HSC
  //Hari|32|
  //Kumar|35|ME,BE,Diploma
  
  val q8df=spark.read.option("delimiter","|").option("header","true")
  .csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/ExplodeArrayExample/ExplodeArrayExample.txt")
  
  val q8df2= q8df.withColumn("qualification", explode(split(col("Education"),","))) 
 
  //to include null values in consideration we can use explod_outer 
  
  val q8df3=q8df.withColumn("qualification", explode_outer(split(col("Education"),","))) 
  //.withColumn("posexploded", posexplode(split(col("Education"),","))) postioning/Indexing of explode column cann be done through this function 
  //but in spark 2.4 its not supported to use with withcolumn instead we can use it in select clause
  //q8df3.show()
 

  
 //Question 9.Consider we have a datafile of students mark list, 
  //we need to transpose subject column and calculate total marks for each student.
  
  val q9df=spark.read.option("delimiter","|").option("header","true").option("inferSchema","true")
  .csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/PivotFunction/Student_pivot.txt")
  
  val q9df2=q9df.groupBy("Roll_No").pivot("Subject").agg(max("Marks"))//.show()
  //val q9df2=q9df.groupBy("Roll_no")
  
  val q9df3=q9df2.withColumn("Total",q9df2("English")+q9df2("History")+q9df2("Maths")+q9df2("Physics")+q9df2("Science"))

  //q9df3.show()
  
  
//Question 10.Consider we have a CSV with a column request having json like data in it.
  //How will you flatten the JSON format string column into a separate columns.
  //.option("multiline", "true")
  val q10df=spark.read.option("header","true").option("escape","\"").option("multiline", "true").option("inferSchema","true")
  .csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/json_use_case/jsonUseCase.txt")
  //q10df.printSchema
 // q10df.show(false)
//Method 1. With Json_tuple
  q10df.select(col("*"),json_tuple(col("request"),"Response")).drop("request")
  .select(col("*"),json_tuple(col("c0"),"MessageId","Latitude","longitude").as(Array("MessageId","Latitude","longitude")))
  .drop("c0")//.show()
  

  
  //Question 11.Consider a Csv file with some duplicate records in it.Our requirement is to find the duplicate rows in spark dataframe and report
  //the output.
  
  val q11df=spark.read.option("header","true").option("delimiter","|")
  .csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/FindDuplicateRecord/Student.txt")
  
  //Method 1. Group by
  
  val groupBycols=q11df.columns
  q11df.groupBy(groupBycols.map(col):_*).count().where("count > 1")//.show()
     
  //Method 2.ranking rownum function using window
  
  val win=Window.partitionBy("Name").orderBy("Age")
  
  val dff=q11df.withColumn("rank",row_number().over(win))
  
  dff.where(col("rank")>1)//.show()
  
  
  //Question 12.Consider an input file with some dummy rows[row no 0 to 6] before actual data.
  //How will you remove those dummy lines and load the data into spark dataframe.
  
  val q2rdd=spark.sparkContext
  .textFile("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/SkipFirstNrowsWhileReading/page.csv", 2)
  .map(x=>x.split(","))
  
 //could be done with mappartitionwithindex...we will do it later
  
  //Question.13. how to check the count of records in each partition
  
  val q13df=spark.read.option("header","true")
  .csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/CheckSkewPartition/BankChurners.csv")
  //q13df.show()
  //use spark partition id
  
  val in_data=q13df.repartition(col("Card_Category"))
  
  val q13finaldf=in_data.withColumn("partitonId", spark_partition_id)
    
  q13finaldf.groupBy("partitonId")
  .count()
  //.select("partitonId", "count")
  //.show()
  
//Question 14. Masksing the columns 
  //Method 1. (below approch working but not perfect)
  
    
    val q14df=spark.read.option("header","true")
  .csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/MaskingColumnEx/mask_data.csv")
  
  def mask_email_function(colVal:String):String={
    val mail_usr=colVal.split("@")(0)
    val n=mail_usr.length()         
    val charlist=mail_usr.toList                      
    val newlist=charlist.map(e=>if (e==charlist(0) || e==charlist(n-1)) e else "*").mkString
    val out=newlist +"@"+colVal.split("@")(1) 
    out
  }
  def mask_mob_function(colval:String):String={
    val n=colval.length()
    val charlist=colval.toList
    val newlist=charlist.map(e=> if (e==charlist(0) || e==charlist(n-1)) e else "*").mkString
    newlist
  }
  //lets register the udf
  spark.udf.register("mask_email",mask_email_function(_:String):String)
  spark.udf.register("mask_mobile",mask_mob_function(_:String):String)
  
  q14df.withColumn("mask_emais", expr("mask_email(email)"))
  .withColumn("mak_mobile",expr("mask_mobile(mobile)"))
  //.show
  
  
   //Method 2. (below approch working but not perfect)
  
    
    val q14df2=spark.read.option("header","true")
  .csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/MaskingColumnEx/mask_data.csv")
  
  
  def mask_email_f(colval:String):String={
    val mail=colval.split("@")(0).toCharArray()
    var a=""
    for (i <- 0 to mail.length-1){
      if (i==0 || i==mail.length-1) { a=a+mail(i)} //when i=o =>a=a  //when i=1 => a* //when i=2 =>a** //when i=3 a**r
      else {a=a+"*"}
      
    }
    return (a+"@"+colval.split("@")(1))
  }

  def mask_phone(colval:String):String={
    
    var a=""
    for (i <- 0 to colval.length-1){
      if (i==0 || i== colval.length-1) (a=a+colval(i))
      else (a=a+"*") 
    }
    return (a)
    
  }
  
   spark.udf.register("mask_email_f",mask_email_f(_:String):String)
   spark.udf.register("mask_phone",mask_phone(_:String):String)
   
   
 
   q14df.withColumn("mask_emails", expr("mask_email_f(email)"))
   .withColumn("mask_phone", expr("mask_phone(mobile)"))
   .drop("email","mobile")
   //.show()
  
   
//Question 16.Consider a spark dataframe having some columns,calculate the total balance amount for each customer based on credit and
//debits.
   
   val q16df=spark.read.option("header","true")
   .csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/PivotFunction/customer_total.csv")
   //.show(5)
  
   //Method 1. we can add a column where we are having debits it will convert the value to minus and then by using group by on customer we can 
   //calculate the total balance
   
  val q16dfa=q16df.withColumn("MkDebitsNegtve", when(col("Transaction Type")==="debit",col("Amount").*(-1)).otherwise(col("Amount")))
   .groupBy("Customer_No").agg(sum("MkDebitsNegtve").as("Total_balance"))
   .select("Customer_No","Total_balance")
   //.show()  
   
   //Method 2. Using pivot(recommended and optimized)

   val q16dfb=q16df.groupBy("Customer_No").pivot("Transaction Type").agg(sum("Amount"))
   .withColumn("Total_balance", col("credit")-col("debit"))
   //.show()
   

   
 //Question 17. Read the data recursively when we will have the multiple csv files under multiple subfolders and check 
 //the particular record is coming from exactly which file 
   
   //val q17df=spark.read.option("header","true").option("recursiveFileLookup","true").csv("")
   
   //to know from which file record occured on particular row
   
  // q17df.withColumn("filename", input_file_name())
   
   
   
//Question 18. Write a script for the below scenario either in PySpark (or) Spark Scala. \
//Note-
//1.Code only using Spark RDD. 
//2.Dataframe or Dataset should not be used
//3.Candidate can use Spark of version 2.4 or above
  
//1. Read the input testfile (Pipe delimited) provided as a "Spark RDD" 
//2. Remove the Header Record from the RDD
//3. Calculate Final_Price:
//     Final_Price = (Size * Price_SQ_FT)
//4. Save the final rdd asTextfile with three fields
//      Property_ID|Location|Final_Price
  
   val q18Rdd=spark.sparkContext
   .textFile("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/Interview_question_on_rdd/Real_estate_info.csv")
   
  //val q18Rdd2=q18Rdd.flatMap(x=>x.split(",").toString())
  
   
  //datawithHeader.foreach(println)
   val datawithHeader=q18Rdd.filter(x=> !x.startsWith("Property_ID"))
   
   val q18Rdd2=q18Rdd.filter(x=> !x.startsWith("Property_ID"))
   .map(x=>
  (x.split(",")(0),x.split(",")(1),x.split(",")(5).toFloat,x.split(",")(6).toFloat
   ))
   
   
   val headerq18=q18Rdd.filter(x=>x.startsWith("Property_ID"))
   .map(x=>(x.split(",")(0),x.split(",")(1),"total_balance"))
   def final_price(d1:Float,d2:Float) ={
     val x=d1 * d2
     x
   }
   //val prefinal=actualData.map(x=>x(0)+ x(1))
   
   //val dataq18=dataRddq18.map(x=>x.split("|"))
   
  
   val prefinal=q18Rdd2.map(x=>(x._1,x._2,(x._3*x._4).toString()))
   
  val finall =headerq18.union(prefinal)
       
//  finall.coalesce(1).foreach(println)
  
  
//Question 19.we have a data like below and we have to read it in spark but have to remove extra element
  
  
  case class SchemaOf(m1:Int,m2:Int,m3:Int)
  val dataq19=spark.read.option("header","true").option("mode","DROPMALFORMED")
  .csv("D:/TrendyTech/Week13_spark_optimization_part1/learntosparkcode-master/learntosparkcode-master/multi/dataframe_question19.csv")
  
//m1,m2,m3
//12,34,56
//23,56,86
//45,67,56,87
//56,77,66
 //dataq19.show()

  
//Question 20.We have a country data like below and we want to below dataset as a output.write the spark code
  
//Id Country
//1 Srilanka
//2 Australia
//3 India
//4 South Africa
//5 Newzealand
//
//
//Expected Output:
//
//Srilanka Vs Australia
//Srilanka Vs India
//Srilanka Vs South Africa
//Srilanka Vs Newzealand
//Australia Vs India
//Australia Vs South Africa
//Australia Vs Newzealand
//India Vs South Africa
//India Vs Newzealand
//South Africa Vs Newzealand
val List20=List(
Row(1,"Srilanka"),
Row(2,"Australia"),
Row(3,"India"),
Row(4,"South Africa"),
Row(5,"Newzealand"))

  val  cricketTeam=StructType(List(
          StructField("Id",IntegerType,true),
          StructField("Country",StringType,true)))
  
val df20=spark.sparkContext.parallelize(List20)
  
val df20a=spark.createDataFrame(df20,cricketTeam)

df20a.join(df20a.as("df2"),df20a.col("Id")!==col("df2.Id"),"cross")
.select(df20a.col("Id"),df20a.col("Country"),col("df2.Country"))
.show()
 

  
  
  
  //Id Country
//1 Srilanka
//2 Australia
//3 India
//4 South Africa
//5 Newzealand
  
  

  

 
  
}