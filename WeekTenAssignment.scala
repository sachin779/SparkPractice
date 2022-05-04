import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext


object WeekTenAssignment extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  val sc=new SparkContext("local[*]","AssignmentWeekTend")
  val ChapterRead=sc.textFile("D:/TrendyTech/Week10_spark_in_depth/Assignment/chapters-201108-004545.csv")
  //(chapterid couresid)
  //ChapterPerCourse will tell us how many chapters we have per course
  
  val ChapterPerCourse=ChapterRead.map(x=> (x.split(",")(1).toInt,1)).reduceByKey((x,y)=> x+y).sortBy(x=>x._1,true)
  //output=>(courseID,numberOFChaptersPerCourse)
  //ChapterPerCourse.collect.foreach(println)
  
  //Step1:Removing Duplicate Views
  
  
  val viewsRead=sc.textFile("D:/TrendyTech/Week10_spark_in_depth/Assignment/views/")
  
  val Views=viewsRead.map(x=> (x.split(",")(0).toInt,x.split(",")(1).toInt)).distinct()
  //this will return tuple of (userid,chapterid)
  
  //step2:Joining to get CourseId in the RDD
  //now after joining you will haver
  //(chapterid ,(CourseId,userId))
  //Step3.Drop the Chapter Id Since it is not required--this will avoid working with tricky nested tuples later on
  
  
  val ViewsJoinCourse=ChapterRead.map(x=>(x.split(",")(0).toInt,(x.split(",")(1).toInt))).join(Views.map(x=>(x._2,x._1))).map(x=>(x._2._2,x._2._1))
  //this will give us (userId,CourseId)
 
  //At the same time we are counting how many chapters of course each user watched ,
  //we will be counting shortly so we can drop a "1" in for each user id
  
  val PostAggregation=ViewsJoinCourse.map(x=>((x._1,x._2),1))
  //Step4-Count Views for User/Course
  //we will do reduce by key now
  val Aggregation=PostAggregation.reduceByKey((x,y)=>x+y)
  //we will get result of Aggregation as below
  //(userId,coureseId),views
  //eg.(14,3),2
  //we can read as userId 14   watched 2 chapters of courseId 3
  
  //Step5-Drop the userId
  val  DropedUsrId=Aggregation.map(x=>(x._1._2,x._2))
  //it will give like below
  //CourseId  Views
  //1          2
  //1          1
  //2          1
  //3          1
  //To rephrase the previous step,we now read this as"someone watched 2 chapters of course 1.
  //Somebody different watched 1 chapter of course1;someone watched 1chapter of course 2,etc".
  
  //Step6:of how many chapters?
  //The scoring is based on what percentage of the total course they watched.So we will need toget the total number of chapters for each 
  //course into our RDD
   
  val getViewsOutof=ChapterPerCourse.join(DropedUsrId).map(x=>(x._1,(x._2._2,x._2._1)))
  //(course_id,(views,numberOFChaptersPerCourse)
  //eg.1      ,(2,3)
  //   1      ,(1,3)
  //someone watched 2 chapter out of 3 
  
  //Step7: Convert to percentage
  
  val PercentageOutOf=getViewsOutof.map(x=>(x._1,x._2._1/x._2._2))
  //this should give
  //CourdeId   percent
  //1            0.6667
  //1            0.3335
  
  //Step8:Convert to scores
  //As desctibed in the problem statement ...percentage are converted to scores
  //If a user watches more than 90% of the course,the course gets 10 points
  //If a user watches >50% but<90%,it scores 4
  //If a user watches >25% but<50%it scores 2
  //Less than25% is no score
  
  val ScoreBasedOnPercentage=PercentageOutOf.map{
    x=> (x._1,if (x._2>.9) 10 else if (x._2>0.5 && x._2<0.9) 4 else if (x._2>0.25 && x._2<0.50) 2 else 0)
  }
  
  //This should give like below
  //CourseID  Score
  //1           4
  //1           2
  
  //Step 9: Sum up this two
  
  val FinalScore=ScoreBasedOnPercentage.reduceByKey((x,y)=>x+y)
  //CourseID Score
  //1           6
  
  
 val title=sc.textFile("D:/TrendyTech/Week10_spark_in_depth/Assignment/titles-201108-004545.csv")
 
 val getTitle=title.map(x=>(x.split(",")(0).toInt,x.split(",")(1)))
 
 val FinalScoreBytitle=getTitle.join(FinalScore).map(x=>(x._2._1,x._2._2))
 
 val SortedOutput=FinalScoreBytitle.sortBy(x=>x._2,true)
 
 //this should give
 //(title,score)
  
  SortedOutput.coalesce(1).sortBy(x =>x._1,true).saveAsTextFile("D:/TrendyTech/Week10_spark_in_depth/Assignment/first_output")
  
   
  
  
  
}