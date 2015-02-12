/*
Creator - Mu Sigma

Objective - Read the pre-processed sessionised data for FFX level -> Calculate the asset categories for each day and the total time spent for each. Also calculate the top 10 articles for each day

Inputs - User has to pass two arguments - week number for which it will be filtered and the masthead for which it will be filtered. 
The sessionised files for the weeks at the masthead level need to be present in mu-sigma-data folder

Outputs - The code outputs the day wise asset categories and their time spent for the masthead. It also returns the top 10 articles viewed by time spent

Assumptions in the code - 
1.) One assumption made here is that the probability of two sessions starting at the exact same millisecond time is very low. Thus, session Start time is used as a proxy for Session ID. The code gives an error whenever Session ID is used.

Modifications - 
This code is kept seperate for now. Once finalized, it will be merged with the view1sankey_metrics_part1.scala code

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

object view1duration {
  type Click = Map[String, String]

  def main(args:Array[String]) {

    val conf = new SparkConf().setAppName("Duration")

    val sc = new SparkContext(conf)

    var wk = args(0).toString
    var masthead = args(1).toString

    val UnconvertedClicksLocation = "s3n://ffx-analytics/mu-sigma-data/" +masthead +"_wk" + wk + "_sessionised_mon/p*"

//Reading as a textfile and deserializing it
    var read_text = sc.textFile(UnconvertedClicksLocation,10)
    var clicks: RDD[Click] = read_text.map(deserialise(_))

    //duration - asset category
//Calculate the time spent on each of the pages. For this, first create a list of all asset categories along with the timestamps for a session.
    var gby_sessionid_articles_timestamp = clicks
	.map(c=>((toDate_try(c.getOrElse("timestamp","").toLong),c.getOrElse("sessionStart","")),(c.getOrElse("timestamp",""),c.getOrElse("assetCategory",""))))
	.groupByKey()
	.map(t=>(t._1,t._2.toList.sorted))

//Get the difference between consecutive page views and hence calculate the time spent on each page
    var gby_sessionid_articles_timeduration = gby_sessionid_articles_timestamp
	.map(t=>((t._1._1), assetCat_duration(t._2)))
	.flatMap(t=>(for(x<-t._2) yield (t._1,x)))
	.map(t=>((t._1,t._2._1),t._2._2.toInt))
	.reduceByKey(_+_)
	.map(c=>("Day","Duration",masthead,c._1._1,c._1._2,c._2))

        gby_sessionid_articles_timeduration.repartition(1).saveAsTextFile(masthead+"_duration_wk" +wk)
        gby_sessionid_articles_timeduration.repartition(1).saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/view1/" + masthead + "_duration_wk" +wk)


/* NOT WORKING YET
          //top 3 articles duration
    var top3_sessionid_articles_timestamp = clicks
        .map(c=>((toDate_try(c.getOrElse("timestamp","").toLong),c.getOrElse("sessionStart","")),(c.getOrElse("timestamp",""),c.getOrElse("title","")
        .replace(" ","#"))))
        .groupByKey()
        .map(t=>(t._1,t._2.toList.sorted))

    var top3_sessionid_articles_timeduration = top3_sessionid_articles_timestamp
        .map(t=>((t._1._1), assetCat_duration(t._2)))
        .flatMap(t=>(for(x<-t._2) yield (t._1,x)))
        .map(t=>((t._1,t._2._1),t._2._2.toLong))
        .reduceByKey(_+_)
        .map(t=>(t._2,t._1))
        .sortByKey(false)
        .take(10)
    var p_top3_sessionid_articles_timeduration = sc.parallelize(top3_sessionid_articles_timeduration,1)
          
//left join and finding url

    var key_time_site_title = clicks
        .map(c=>((toDate_try(c.getOrElse("timestamp","").toLong),c.getOrElse("title","").replace(" ","#").toString),(c.getOrElse("url",""))))
        .distinct()
    var finding_url = p_top3_sessionid_articles_timeduration
        .map(t=>(t._2,t._1)).
        leftOuterJoin(key_time_site_title)
        .map(t=>("Day","Duration_Article",masthead,t._1._1,t._1._2,t._2._2.getOrElse(""),t._2._1))

    finding_url.repartition(1).saveAsTextFile(masthead + "_article_duration_wk" + wk)
    finding_url.repartition(1).saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/view1/" + masthead + "_article_duration_wk" + wk)
*/

}

//Takes as input a list containing the assets and timestamps and returns the time spent on each asset
  var returned_list = new ListBuffer[(String,String)]()
  def assetCat_duration(x:List[(String,String)]) : List[(String,String)] = {
    for(i<-0 until x.length-1) {
      var (a,b) = x(i)
      var (c,d) = x(i+1)
      var diff = c.toLong - a.toLong
      returned_list+= Pair(b,diff.toString)
    }
    returned_list.toList
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




