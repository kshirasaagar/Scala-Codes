/*
Creator - Mu Sigma

Objective - Read a month long data of any masthead -> filter the data for relevant week -> remove auto reload pages -> flag the session -> save it as intermediate pre-processed data
Inputs - User has to pass two arguments - week number for which it will be filtered and the masthead for which it will be filtered. 

Outputs - The code outputs sessionised data after removing the auto-reload views for a week worth of data. It is saved in mu-sigma-data folder on S3 with relevant name.

Assumptions in the code - 
1.) One key assumption made in the code is that there is a very low probability that the same visitor has two pageviews at exactly the same timestamp. This assumption is used when auto-reload views are joined back to the original click data on timestamp and tracking ID.
2.) In order to remove auto-reloads, data is filtered for only homepage index views. An assumption made here is that visitors re-loading the homepage at exactly 5 min manually is very low. This assumption is also accounted for in the probability equation for removing auto-reloads.

Modifications - 
Slight modifications need to be made in the code manually currently - 
1.) Based on the week number inputted, the months for which the raw data is to be read has to be manually updated. Currently it is set for November, 2014
2.) Based on the masthead selected, the value of Asset Category has to be updated. Set it to blank or any junk value for mastheads other than Age and SMH. For Age, make it "The Age" and for SMH, make it "The Sydney Morning Herald"
These will be automated as well if possible.

*/

import java.security.MessageDigest

import com.fasterxml.jackson.core.JsonParseException
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._

import org.apache.spark.SparkContext._
import scala.collection.immutable.Map

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.broadcast.Broadcast
import ua_parser.{Client, Parser}
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
import org.json4s.DefaultFormats

import java.util.Calendar
import java.util.TimeZone
import java.util.Locale

object auto_reload {
  type Click = Map[String, String]


  def main(args:Array[String]) {

    val conf = new SparkConf().setAppName("Sessionise")


    val sc = new SparkContext(conf)

    //just for declaring variables
    var i = 0
    var masthead = args(1).toString
    var wk = args(0).toString

    var ClicksLocation = "s3n://ffx-analytics/all-interactions/11-2014-" + masthead + "*view*"

    var read_text = sc.textFile(ClicksLocation,50)

    var clicks: RDD[Click] = (read_text
        .filter((l: String) => !(l contains "00000000-0000-0000-0000-000000000000"))
        .map(expandRecord(_))
    )

//Filter for the relevant week
    var week_clicks = clicks
        .map(c=>(toWeek(c.getOrElse("timestamp","").toString.toLong),c))
        .filter(c=>(c._1 == wk))
        .map(c=>(c._2))

  /*
   code for auto reload goes here
   */
//Get the index views from the raw click data
    var trackid_timestamp_asset = week_clicks.map(c=>(c.getOrElse("trackingId",""),c.getOrElse("timestamp",""),c.getOrElse("assetType","").toString,c.getOrElse("assetCategory","").toString)).filter(t=>(t._3 == "Index" && t._4 == "The Sydney Morning Herald"))

    var rescale = 0.996
//Get assets ordered by timestamp for each tracking ID
    var groupby_trackid = trackid_timestamp_asset
	.map(t=>(t._1,(t._2)))
	.groupByKey()
	.map(t=>(t._1,convert_to_pair(t._2.toList.sorted)))

//Smoothen the exponential curve and flag based on the probability equations and difference from the peak of 5 min whether a click is auto-reload
    var trackingid_timestamp_asset = groupby_trackid
	.flatMap(t=>(for(x<-t._2) yield(t._1,x)))
	.map(t=>(t._1, t._2._1, t._2._2.toString.toLong - t._2._1.toString.toLong, t._2._2.toString.toLong - t._2._1.toString.toLong - 300000L))
	.map(t=>(t,(if(t._3.toLong>0 && t._4.toLong>0) rescale * math.exp(-math.pow(t._4.toDouble/120000.0,2))
	      else if(t._3.toLong>0) rescale * math.exp(-math.pow(t._4.toDouble/140000.0,2))
	      else 0)))
	.map(t=>(t,if (t._2==0) 0 else if(t._2>Random.nextDouble()) 1 else 0))

//Based on the mix-model results from David, flag if a click is auto-reload
    var autoreload = trackingid_timestamp_asset
	.filter(t=>(t._2 == 1 && Random.nextDouble()>0.14))
	.map(t=>((t._1._1._1, t._1._1._2), 1))

//Join the auto-reload clicks to the original data and filter out the auto-reloads
    var after_autoreload = week_clicks
	.map(c=>((c.getOrElse("trackingId",""),c.getOrElse("timestamp","")),c))
	.leftOuterJoin(autoreload)
	.filter(t=>(t._2._2.getOrElse("") == ""))
	.map(c=>(c._2._1))

//Group all the timestamps for a tracking ID, to be used for sessionising the data
    var session = after_autoreload
        .map(c=>(c.getOrElse("trackingId",""),c.getOrElse("timestamp","")))
        .groupByKey()
        .map(t=>(t._1,t._2.toList.sorted))
        .map(t=>(t._1,split(t._2)))

//Flatten the list to get list of timestamps as seperate rows
    var track_individual_list = session.flatMap(t=>(for(xs<-t._2)yield(t._1,xs)))

//Map the session ID back to the original click data. Assumption - negligible chances of a tracking ID having two clicks at the exact same timestamp
        var withsessionid = track_individual_list
               .map(t=>(t,t._2.head + t._1,t._2.head,t._2.last,(t._2.last.toLong-t._2.head.toLong)/1000,t._2.length))
               .flatMap(t=>(for(x<-t._1._2) yield (t._1._1,x,t._2,t._3,t._4,t._5,t._6)))
               .map(t=>((t._1,t._2),(t._3,t._4,t._5,t._6,t._7)))

//Add the sessionID and referer to the click data using updated statement
      var sessionised_clicks = after_autoreload
          .map(c=>((c.getOrElse("trackingId",""),c.getOrElse("timestamp","")),c))
          .join(withsessionid)
          .map(c=>(c._2._1.updated("sessionId",c._2._2._1.toString)
                .updated("sessionDuration",c._2._2._4.toString)
                .updated("sessionStart",c._2._2._2.toString)
                .updated("sessionEnd",c._2._2._3.toString)
                .updated("numPage",c._2._2._5.toString)))
          .map(c=>(c,mapReferer(c.getOrElse("referer","").toString,c.getOrElse("url","").toString)))
          .map(c=> (c._1.updated("finalReferer",c._2.toString)))
        //sessionised_clicks.take(10).foreach(println)

//Save the output with relevant filenames
        var save_file = masthead + "_wk" + wk + "_sessionised_mon"
        var save = sessionised_clicks
        .map(serialise(_))
        .repartition(50)

        save.take(50).foreach(println)


        save.saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/"+save_file, classOf[GzipCodec])
        save.saveAsTextFile("mu-sigma-data/"+save_file,classOf[GzipCodec])


}

var first:String=""

var second:String=""

var make_list = new ListBuffer[(String,String)]()



def convert_to_pair(x:List[String]) : List[(String,String)] = {

  make_list.clear()
  if(x.length ==1) {

    first = x(0)
    make_list += Pair(first,first)

  }else {

  for(i<-0 until x.length-1) {

    first = x(i)
    second = x(i+1)
    make_list += Pair(first,second)

  }
    first = x(x.length-1)
    make_list +=Pair(first,first)
  }
  make_list.toList
}


    def toWeek(timestamp:Long): String = {

      //Date date1 = new Date(timestamp)
      //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); // the format of your date
      //String formattedDate = sdf.format(date1);
      //   return formattedDate

      val mydate = Calendar.getInstance(TimeZone.getTimeZone("Australia/Sydney"), Locale.GERMANY)
      mydate.setTimeInMillis(timestamp)
      val returndate = mydate.get(Calendar.WEEK_OF_YEAR)
      return returndate.toString
    }

    var fs:String=""
    var b = new ListBuffer[String]()
    def split[A](xs: List[String]): List[List[String]]= {

      var n =0
      if (xs.size==1) {
        fs = xs(0)
        return List(xs)
      }
      else {
        val loop = new Breaks;
        loop.breakable {
          for (i <- 0 until xs.length-1) {
            var diffs = xs(i+1).toLong- xs(i).toLong
            if (diffs.toLong > 1800000) {
              n = i+1
              loop.break
            }
          }
        }
        if(n==0) return List(xs)
          val (ys, zs) = xs.splitAt(n)

         //println("ys contents begin here")
        // ys.foreach(println)
        // println("ys contents end here")
        // println("zs contents begin")
        // zs.foreach(println)
        // println("zs contents end")


        ys :: split(zs)
      }

    }
      var d = new ListBuffer[String]()
      def lag_asset(a:Array[String],b:Array[String]):Array[String]= {
        d.clear()
        for(i<-0 until a.length) {
          if(a(i).contentEquals(b(i))) {
            d+="Same_asset"
          }else {
            d+="Not_Same"
          }
        }
        return d.toArray
      }

      def check(a:List[List[String]]):List[String]= {
        b.clear()
        for(i<-0 until a.length) {b+= (i+1).toString}

        return b.toList
      }

      def sub(a:Array[String],b:Array[String]):Array[String]= {

        for(i<-0 until a.length) {
          var diff = a(i).toLong - b(i).toLong
          d += diff.toString
        }
        return d.toArray
      }


    def mapReferer(referer:String, url:String):String = {

	if (referer == "direct") "DIRECT"
	else if ((referer == "") && (!("""www\.smh\.com\.au""".r.findAllIn(url).isEmpty))) "DIRECT"
	else if ((referer == "") && (!("""www\.theage\.com\.au""".r.findAllIn(url).isEmpty))) "DIRECT"
	else if (!("""www\.google\..*""".r.findAllIn(referer).isEmpty)) "GOOGLE"
	else if (!("""\*\.bing\..*""".r.findAllIn(referer).isEmpty)) "BING"
	else if (!("""\*\.yahoo\..*""".r.findAllIn(referer).isEmpty)) "YAHOO"
	else if (!("""\*\.drive\..*""".r.findAllIn(referer).isEmpty)) "DRIVE"
	else if (!("""www\.smh\.com\.au""".r.findAllIn(referer).isEmpty)) "SMH"
	else if (!("""www\.theage\.com\.au""".r.findAllIn(referer).isEmpty)) "THEAGE"
	else if (!("""traffic\.outbrain\.com""".r.findAllIn(referer).isEmpty)) "OUTBRAIN"
	else if (!(""".*reddit\.com""".r.findAllIn(referer).isEmpty)) "REDDIT"
	else if (!(""".*twitter\.com""".r.findAllIn(referer).isEmpty)) "TWITTER"
	else if (!("""linkedin\.com""".r.findAllIn(referer).isEmpty)) "LINKEDIN"
	else if (!(""".*facebook\.com""".r.findAllIn(referer).isEmpty)) "FACEBOOK"
	else "referral"
   }

          def parseWithErrors(jsonRecord:String):Option[Map[String,JValue]] = {

            implicit val formats = DefaultFormats

            try {
              Some(parse(jsonRecord).extract[Map[String,JValue]])
            } catch {
              case e: JsonParseException => {
                val index = e.getLocation.getColumnNr - 2

                // println(jsonRecord)

                jsonRecord.charAt(index) match {
                  case '\'' => parseWithErrors(jsonRecord.patch(index - 1, "", 1))
                  case 'v' => parseWithErrors(jsonRecord.patch(index - 1, "\\u0007", 2))
                  case 'a' => parseWithErrors(jsonRecord.patch(index - 1, "\\u0013", 2))
                  case _ => parseWithErrors(jsonRecord.patch(index - 1, "", 1))
                }
              }
              case _: Throwable => None
            }
          }

          def expandRecord(jsonRecord:String):Map[String,String] = {

            val digest = MessageDigest.getInstance("MD5")

            val partParsed:Option[Map[String,JValue]] = parseWithErrors(jsonRecord)

            // Now, walk partParsed and convert each of the JValues to their primitive types.
            // This is to circumvent https://github.com/json4s/json4s/issues/124

            def reparse(v:JValue):Any = v match {
              case JString(s) => s
              case JInt(i)    => i.toString()
              case JArray(o)  => (o
                .asInstanceOf[List[JObject]]
                .map(
                  (pair:JObject) => {
                    // we know that the pair has name, value keys
                    val p = (pair
                      .obj
                      .toMap
                      .mapValues(_.values.asInstanceOf[String])
                    )

                  Map[String,String](p("name")  -> p("value"))
                }
              )
            .foldLeft(Map[String,String]())(_ ++ _)
          )
        case _          => v
      }

      partParsed match {
        case Some(s)  => {
          val withExtras = s.mapValues(reparse)
          val trackingId = digest.digest(withExtras("trackingId").asInstanceOf[String].getBytes).map("%02x".format(_)).mkString
            val extras = withExtras.getOrElse("extras", Map()).asInstanceOf[Map[String, String]]
            (withExtras - "extras").updated("trackingId", trackingId).asInstanceOf[Map[String, String]] ++ extras
          }
          case None     => Map[String,String]()
        }
      }

      def serialise(click:Map[String,String]):String = {

        implicit val formats = DefaultFormats

        write(click)
      }
}



