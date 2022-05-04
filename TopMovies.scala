import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

//Problem statement:
//1.Atleast 1000 people should have rated for moview
//2.average rating should >4.5
object TopMovies extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc =new SparkContext("local[*]","wordcout")

  val ratingsGiven=sc.textFile("D:/TrendyTech/Week11_spark_api_structured/ratings-201019-002101.dat")
  
  //we need only two columns for our processing (movieId,rating)
  
  
  val GetMovieIdnRating=ratingsGiven.map( x=>{
    val fields=x.split("::")
    ((fields)(1),fields(2))  
  })
  
  //for newmapped below is the input
  //(1193,5)
  //(1193,3)
  //(1193,4)
  
  //expected output
  //(1193,(5.0,1.0))
  //(1193,(3.0,1.0))
  //(1193,(4.0,1.0))
  
  val newmapped=GetMovieIdnRating.mapValues(x=>(x.toFloat,1.0))
  
  //now input for next rdd is 
  //(1193,(5.0,1.0))
  //(1193,(3.0,1.0))
  //(1193,(4.0,1.0))
  
  //expected output
  //(1193,(12.0,3.0))
  
  val nextRdd=newmapped.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  
  //now we have to filter the movies have rated more than 1000
  
  val filterRdd=nextRdd.filter(x=>x._2._2 >1000)
  
  //now we have the outputs like bwlow
  //(1193,(1200.0,3000.0))
  
  //we want to do average for our next condition
  
  val finalRdd=filterRdd.mapValues(x=>x._1/x._2).filter(x=>x._2>4.0)
  //(movieID,avg_ratings)
  
  val Moviename=sc.textFile("D:/TrendyTech/Week11_spark_api_structured/movies-201019-002101.dat")
  //finalRdd.collect.foreach(println)
    //get the movie name for all these movie id

    val GetMovieIdnName=Moviename.map( x=>{
    val fields=x.split("::")
    ((fields(0),fields(1)))  
  })
  
  //val movieBroadcast=sc.broadcast(GetMovieIdnName)
  
  val GetTopMovies=finalRdd.join(GetMovieIdnName).sortBy(x=>x._2._1,false)
  
  //val GetTopMoviesWithName=GetTopMovies.map(x=>(x._2._1,x._2._2))
  GetTopMovies.collect.foreach(println)
  
  scala.io.StdIn.readLine()
 

  
}