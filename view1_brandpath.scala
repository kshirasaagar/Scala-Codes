/*
Creator - Mu Sigma

Objective - Read the pre-processed sessionised data for FFX level -> Calculate brand path from it

Inputs - User has to pass one argument - week number for which the brandpath will be calculated. The sessionised files for the weeks at FFX level need to be present in mu-sigma-data folder

Outputs - The code outputs the day wise brand paths and the count of sessions or visits following each path

Assumptions in the code - 
1.) Assumption made here is that if the same tracking ID visits two different mastheads, it is the path that they followed. We are not looking in detail at the value of referer and url to identify if the source was internal
2.) The path longer than 5 sites is limited to just the first 5 sites visited. This is done since there are a few instances of sites coming up alternatively(smh, age, smh, age, smh, age....). This could have been happening due to auto-reload or due to users visiting multiple sites. But since the # occurences of this was very low, we limited it to top 5, considering it a data anomaly
3.) There is no need to track mobile and desktop sites seperately. Tracking them as same as of now

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

object view1path {
  type Click = Map[String, String]

  def main(args:Array[String]) {

    val conf = new SparkConf().setAppName("BrandPath")

    val sc = new SparkContext(conf)
    var wk = args(0).toString

    val UnconvertedClicksLocation = "s3n://ffx-analytics/mu-sigma-data/FFX_wk" + wk + "_sessionised_mon/p*"

//Reading as a textfile and deserializing it
    var read_text = sc.textFile(UnconvertedClicksLocation,10)
    var clicks: RDD[Click] = read_text.map(deserialise(_))

//brandpath - get the site values sorted by timestamp for each session -> count the # sessions following a particular path.
    var brand_path = clicks
        .map(c=>((toDate_try(c.getOrElse("timestamp","").toLong),c.getOrElse("sessionId","")),(c.getOrElse("timestamp","").toString.toLong,remove_m_fromsite(c.getOrElse("site","")))))
        .groupByKey()
        .map(t=>(t._1,t._2.toList.sorted))
        .flatMap(t=>(for(xs<-t._2) yield (t._1,xs)))
        .map(t=>(t._1,t._2._2))
        .groupByKey()
        .map(t=>(t._1,t._2.toList))
        .map(t=>(t._1,compressFunctional(t._2)))
        .map(t=>(t._1,t._2.take(5)))
        .map(t=>((t._1._1,t._2),1))
        .reduceByKey(_+_)


      brand_path.repartition(1).saveAsTextFile("brand_path_wk" +wk)
      brand_path.repartition(1).saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/view1/brand_path_wk" +wk)

}

//Removes the m. prefix from the site value
 def remove_m_fromsite(x:String):String = {
   var s:String=x
   if(x.equalsIgnoreCase("m.smh")) {
     s= "smh"
   }
   if(x.equalsIgnoreCase("m.theage")) {
     s= "theage"
   }
   if(x.equalsIgnoreCase("m.drive")) {
     s= "drive"
   }
   if(x.equalsIgnoreCase("m.traveller")) {
     s= "traveller"
   }
   s
 }

//This function removes the consecutive duplicates. Thus, {age, age, smh, smh, age, age} will become {age, smh, age}
 def compressFunctional[A](ls: List[A]): List[A] =
    ls.foldRight(List[A]()) { (h, r) =>
    if (r.isEmpty || r.head != h) h :: r
    else r
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



