/*
Creator - Mu Sigma

Objective - Read the pre-processed sessionised data for FFX level -> Calculate the asset categories for each day and the total page views and the visits for each. Also calculate the top 10 articles for each day

Inputs - User has to pass two arguments - week number for which it will be filtered and the masthead for which it will be filtered. 
The sessionised files for the weeks at the masthead level need to be present in mu-sigma-data folder

Outputs - The code outputs the day wise asset categories and their total visits and pageviews for the masthead. It also returns the top 10 articles for each day based on the two metrics

Assumptions in the code - 
1.) One assumption made here is that the probability of two sessions starting at the exact same millisecond time is very low. Thus, session Start time is used as a proxy for Session ID. The code gives an error whenever Session ID is used.

Modifications - 
None as of now
*/
import java.security.MessageDigest
import com.fasterxml.jackson.core.JsonParseException
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}

import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._

import org.apache.spark.SparkContext._
import scala.collection.immutable.Map

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.broadcast.Broadcast
//import ua_parser.{Client, Parser}
import org.apache.spark.rdd.RDD
import scala.util.Random
import scala.util.matching.Regex
import Ordering.Implicits._
import Numeric.Implicits._
import scala.math._
import scala.collection.mutable.ListBuffer
import java.net.URL
import scala.util.control._
import scala.io.Source
import com.github.nscala_time.time.Imports._

import java.util.Calendar
import java.util.TimeZone
import java.util.Locale

object view1metrics {
  type Click = Map[String, String]

   val conf = new SparkConf().setAppName("Enricher")

       val sc = new SparkContext(conf)
  def main(args:Array[String]) {

    var wk = args(0).toString
    var masthead = args(1).toString

    val UnconvertedClicksLocation = "s3n://ffx-analytics/mu-sigma-data/" + masthead + "_wk" + wk + "_sessionised_mon/p*"

    //Reading as a textfile and deserializing it
    var read_text = sc.textFile(UnconvertedClicksLocation,10)
    var clicks: RDD[Click] = read_text.map(deserialise(_))

//page views - asset category
    var timestamp_site_assetCategory = clicks
        .map(c=>((toDate_try(c.getOrElse("timestamp","").toLong),c.getOrElse("assetCategory","")),1)).reduceByKey(_+_)
        .map(c=>("Day","PageViews",masthead,c._1._1,c._1._2,c._2))

    timestamp_site_assetCategory.repartition(1).saveAsTextFile(masthead + "_pageviews_wk" +wk)
    timestamp_site_assetCategory.repartition(1).saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/view1/" + masthead + "_pageviews_wk" +wk)

//page views - Top 10 articles
    var timestamp_site_article = clicks
        .map(c=>((toDate_try(c.getOrElse("timestamp","").toLong),c.getOrElse("title","").replace(" ","#"),c.getOrElse("url","")),1))
        .reduceByKey(_+_)
    var days =  timestamp_site_article.map(c=>c._1._1).distinct().toArray
    var top10_view = extract_top_by_day_view(days,timestamp_site_article,masthead)

    top10_view.saveAsTextFile(masthead + "_article_pageviews_wk" +wk)
    top10_view.saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/view1/" + masthead + "_article_pageviews_wk" +wk)

//Sessions - asset category
 var sessions = clicks
    .map(c=>(toDate_try(c.getOrElse("timestamp","").toLong),c.getOrElse("assetCategory",""),c.getOrElse("sessionStart","")))
    .distinct()
    .map(c=>((c._1,c._2),1))
    .reduceByKey(_+_)
    .map(c=>("Day","Visits",masthead,c._1._1,c._1._2,c._2))

    sessions.repartition(1).saveAsTextFile(masthead + "_visits_wk" +wk)
    sessions.repartition(1).saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/view1/" + masthead + "_visits_wk" +wk)

//Sessions - Top 10 articles
    var sessions_article = clicks
        .map(c=>(toDate_try(c.getOrElse("timestamp","").toLong),c.getOrElse("title","").replace(" ","#"),c.getOrElse("url",""),c.getOrElse("sessionStart","")))
        .distinct()
        .map(c=>((c._1,c._2,c._3),1))
        .reduceByKey(_+_)

    var top10_visit = extract_top_by_day_visit(days,sessions_article,masthead)
    top10_visit.saveAsTextFile(masthead + "_article_visits_wk" +wk)
    top10_visit.saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/view1/" + masthead + "_article_visits_wk" +wk)

    //sc.parallelize(sessions_article,1).saveAsTextFile(masthead + "_article_visits_wk" +wk)
    //sc.parallelize(sessions_article,1).saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/view1/" + masthead + "_article_visits_wk" +wk)

}

//This function loops through the data for all the days and returns the top 10 articles for each of the day
  def extract_top_by_day_visit(x:Array[String],y:RDD[((String,String,String),Int)],masthead:String):RDD[(String,String,String,String,String,String,Int)] ={
    var d = ""
    var duration_top10 = y.filter(t=>(t._1._1 == "")).map(c=>(c._2,c._1)).map(c=>("Day","Visit_article",masthead,c._2._1,c._2._2,c._2._3,c._1))
    for (d <- x) {
          var top10 = y.filter(t=>(t._1._1 == d)).map(c=>(c._2,c._1)).sortByKey(false).map(c=>("Day","Visit_article",masthead,c._2._1,c._2._2,c._2._3,c._1)).take(10)

          var day_top10 = sc.parallelize(top10,1)
          duration_top10 = duration_top10 ++ day_top10
         }
         duration_top10
  }

//This function loops through the data for all the days and returns the top 10 articles for each of the day
  def extract_top_by_day_view(x:Array[String],y:RDD[((String,String,String),Int)],masthead:String):RDD[(String,String,String,String,String,String,Int)] = {
      var d = ""
      var view_top10 = y.filter(t=>(t._1._1 == "")).map(c=>(c._2,c._1)).map(c=>("Day","View_article",masthead,c._2._1,c._2._2,c._2._3,c._1))
      for (d <- x) {
            var top10 = y.filter(t=>(t._1._1 == d)).map(c=>(c._2,c._1)).sortByKey(false).map(c=>("Day","View_article",masthead,c._2._1,c._2._2,c._2._3,c._1)).take(10)

            var day_top10 = sc.parallelize(top10,1)
            view_top10 = view_top10 ++ day_top10
      }
      view_top10
  }


//Deserialize the raw data read as json
  def deserialise(json:String):Click = {
    implicit val formats = DefaultFormats

    parse(json).extract[Click]
  }

//convert the timestamp to a calendar instance and extract the Day of the month from it.
  def toDate_try(timestamp:Long): String = {

    val mydate = Calendar.getInstance(TimeZone.getTimeZone("Australia/Sydney"))
    mydate.setTimeInMillis(timestamp)
    val returndate = mydate.get(Calendar.DAY_OF_MONTH)
    return returndate.toString
  }


//Convert the timestamp to a calendar instance and extract the week of the year from it. Locale used is Germany since the week has to start from Monday and not Sunday.
  def toWeek(timestamp:Long): String = {

    val mydate = Calendar.getInstance(TimeZone.getTimeZone("Australia/Sydney"), Locale.GERMANY)
    mydate.setTimeInMillis(timestamp)
    val returndate = mydate.get(Calendar.WEEK_OF_YEAR)
    return returndate.toString
  }


}




