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

object dayweek {
  type Click = Map[String, String]

  def main(args:Array[String]) {

    val conf = new SparkConf().setAppName("view2_metrics")

    val sc = new SparkContext(conf)
    //Read the weekly split data. In case overall Fairfax data has to be read(i.e. for all mastheads), replace the smh with *
    var wk = args(0).toString
    var masthead = args(1).toString
//    val UnconvertedClicksLocation = "s3n://ffx-analytics/mu-sigma-data/FFX_wk45_sessionised_monn/p*.gz"
    val UnconvertedClicksLocation = "s3n://ffx-analytics/mu-sigma-data/" + masthead + "_wk" + wk + "_sessionised_mon/p*"


    //Reading as a textfile and deserializing it
    var read_text = sc.textFile(UnconvertedClicksLocation,500)
    var clicks: RDD[Click] = read_text.map(deserialise(_))


    //Extract the first view of every session. This is done by extracting those views where timestamp is equal to the first timestamp of a session. Assumption - negligible of a tracking IF having two clicks at the exact same timestamp. 
    val session_head = clicks.map(c=>(c,c("timestamp"),c("sessionStart"))).filter(t=>(t._2.equals(t._3))).map(t=>(t._1))
/*
    val fid_numpage_session = session_head.map(c=>(c("trackingId").toString, isIndex(c("url").toString), depth(c("numPage").toString.toInt), toWeekend(c("timestamp").toString.toLong), toDate_try(c("timestamp").toString.toLong) ))

  val session_freq = fid_numpage_session.map(c=>(c._1,1)).reduceByKey(_+_)
  val weekend_freq = fid_numpage_session.filter(c=>(c._4 == true)).map(c=>(c._1,1)).reduceByKey(_+_)
  val weekday_freq = fid_numpage_session.filter(c=>(c._4 == false)).map(c=>(c._1,1)).reduceByKey(_+_)
  val index_freq = fid_numpage_session.filter(c=>(c._3 == "SINGLE" && c._2 == true)).map(c=>(c._1,1)).reduceByKey(_+_)
  val depth_1_freq = fid_numpage_session.filter(c=>(c._3 == "SINGLE")).map(c=>(c._1,1)).reduceByKey(_+_)
  val depth_2_freq = fid_numpage_session.filter(c=>(c._3 == "2-4 PAGES")).map(c=>(c._1,1)).reduceByKey(_+_)
  val depth_3_freq = fid_numpage_session.filter(c=>(c._3 == ">4 PAGES")).map(c=>(c._1,1)).reduceByKey(_+_)
  val spread_flag = fid_numpage_session.map(c=>(c._1,c._5)).groupByKey().map(c=>(c._1,c._2.toList.distinct.length))

  val memberid_trackingid = clicks.map(c=>(c("memberId"),c("trackingId"))).filter(t=>t._1 != "0").distinct
    val member_clearer = memberid_trackingid.map(t=>(t._1,1)).reduceByKey(_+_).filter(t=>(t._2 > 1 && t._2 < 6)).join(memberid_trackingid).map(t=>(t._2._2,"cookie")).distinct
    val ip_ua_disttracking = clicks.map(c=>(c("ipAddress"),c("userAgent"),c("trackingId"))).distinct.map(t=>((t._1,t._2),t._3))
    val ip_ua_count = ip_ua_disttracking.map(t=>(t._1,1)).reduceByKey(_+_).filter(t=>(t._2 > 1 && t._2 < 6)).join(ip_ua_disttracking).map(t=>(t._2._2,"cookie")).distinct
    var pattern_tids = member_clearer ++ ip_ua_count
    var cookie_clearer = pattern_tids.distinct

    val trackingIds = fid_numpage_session.map(c=>(c._1,1)).distinct

    val repeat_rawdata = trackingIds.leftOuterJoin(session_freq).leftOuterJoin(weekend_freq).leftOuterJoin(weekday_freq).leftOuterJoin(index_freq).leftOuterJoin(depth_1_freq).leftOuterJoin(depth_2_freq).leftOuterJoin(depth_3_freq).leftOuterJoin(cookie_clearer).leftOuterJoin(spread_flag)

    val repeat_raw_cleaned = repeat_rawdata.map(c=>(c._1, c._2._1._1._1._1._1._1._1._1._2.getOrElse(-1), c._2._1._1._1._1._1._1._1._2.getOrElse(0), c._2._1._1._1._1._1._1._2.getOrElse(0), c._2._1._1._1._1._1._2.getOrElse(-1), c._2._1._1._1._1._2.getOrElse(0), c._2._1._1._1._2.getOrElse(0), c._2._1._1._2.getOrElse(0), c._2._1._2.getOrElse("not"), c._2._2.getOrElse(-1)))
    //format - 1tracking id, 2sesion_frequency, 3weekend_frequency, 4weekday_frequency, 5index_frequency, 6singlePV_frequency, 72-4PV_frequency, 8>4PV frequency, 9cookie_clearer flag, 10count of days he was active on.

    val repeat_flagged = repeat_raw_cleaned.map(c=>(c._1, 
       if(c._2 > 5 && (c._3.toDouble/c._2.toDouble) > 0.2 && (c._3.toDouble/c._2.toDouble) < 0.8 && c._10 >= 3 && (c._7 + c._8) > c._6 && c._8 > c._7) "HABITUAL ENGAGED VISITORS"
       else if(c._2 < 6 && c._2 > 1 && (c._3.toDouble/c._2.toDouble) > 0.2 && (c._3.toDouble/c._2.toDouble) < 0.8 && c._10 >= 3 && (c._7 + c._8) > c._6) "HABITUAL NON-ENGAGED VISITORS"
       else if (c._2 > 1 && c._5 != -1 && (c._5.toDouble/c._2.toDouble) > 0.5) "UNENGAGED VISITORS"
       else if (c._2 == 1 && c._9 == "not") "VISITORS BY CHANCE"
         else if (c._2 == 1 && c._9 != "not") "COOKIE CLEARER"
           else if (c._2 > 1 && (c._3.toDouble/c._2.toDouble) >= 0.8) "AT HOME READERS"
           else if (c._2 > 1 && (c._3.toDouble/c._2.toDouble) <= 0.2) "AT OFFICE READERS"
           else if (c._2 > 1 && (c._6 + c._7) > c._8 && (c._5.toDouble/c._2.toDouble) <= 0.5) "MISSION READERS"
           else if (c._2 > 5 && c._10 < 3) "EVENT READERS"
           else "UNFLAGGED"
         ))

//       repeat_flagged.map(c=>(c._2,1)).reduceByKey(_+_).take(15).foreach(println)

       //    repeat_rawdata.repartition(1).saveAsTextFile("mu-sigma-data/smh_wk48_repeat_raw",classOf[GzipCodec])
       val head_clicks_enriched = session_head.map(c=>(c("trackingId"),c)).leftOuterJoin(repeat_flagged).map(c=>(c._2._1.updated("repeatFlag",c._2._2.getOrElse("UNFLAGGED").toString)))

     val trend_line = head_clicks_enriched
     .map(c=>((c("repeatFlag"), toDate_try(c("timestamp").toString.toLong)),c("trackingId")))
       .distinct
       .map(c=>(c._1,1))
       .reduceByKey(_+_)

       trend_line.repartition(1).saveAsTextFile("mu-sigma-data/smh_wk45_repeat_trend",classOf[GzipCodec])
      
*/

      val fid_numpage_session = session_head
      .map(c=>(c("trackingId").toString,c("sessionId").toString,c("numPage").toString,c("sessionDuration").toString, toDate_try(c("timestamp").toString.toLong),toWeek(c("timestamp").toString.toLong) ))

//Calculate the distinct tracking IDs for count of visitors
      val weekly_cnt_visitors =  fid_numpage_session
      .map(t=>(t._6,t._1))
      .distinct
      .map(t=>(t._1,1))
      .reduceByKey(_+_)
      .map(c=>("Week_visitor",masthead,c._1,c._2.toDouble))

      val daily_cnt_visitors =  fid_numpage_session
      .map(t=>(t._5,t._1))
      .distinct
      .map(t=>(t._1,1))
      .reduceByKey(_+_)
      .map(c=>("Daily_visitors",masthead,c._1,c._2.toDouble))

      println("Visitors")

//Calculate count of distinct session IDs for count of visits
      val weekly_cnt_visits =  fid_numpage_session
      .map(t=>(t._6,1))
      .reduceByKey(_+_)
      .map(c=>("Week_visit",masthead,c._1,c._2.toDouble))

      val daily_cnt_visits =  fid_numpage_session
      .map(t=>(t._5,1))
      .reduceByKey(_+_)
      .map(c=>("Daily_visit",masthead,c._1,c._2.toDouble))

      println("Visits")
//      cnt_visits.take(5).foreach(println)

//Keeping only those sessions where the numpages > 1
      val weekly_numpage = fid_numpage_session
      .map(t=>(t._6,t._3.toDouble))
      .filter(t=>(t._2 != 1.0))
      .groupByKey()
      .map(t=>("Week_numpage",masthead,t._1,(t._2.toList.sum/t._2.toList.length.toDouble).toDouble))

      val daily_numpage = fid_numpage_session
      .map(t=>(t._5,t._3.toDouble))
      .filter(t=>(t._2 != 1.0))
      .groupByKey()
      .map(t=>("Daily_numpage",masthead,t._1,(t._2.toList.sum/t._2.toList.length.toDouble).toDouble))

      println("numpage")
//      numpage.take(5).foreach(println)

//Keeping only those sessions where the numpages > 1
      val weekly_session_dur = fid_numpage_session
      .map(t=>(t._6,t._4.toDouble))
      .filter(t=>(t._2 != 0.0))
      .groupByKey()
      .map(t=>("Week_session",masthead,t._1,(t._2.toList.sum/t._2.toList.length.toDouble).toDouble))

      val daily_session_dur = fid_numpage_session
      .map(t=>(t._5,t._4.toDouble))
      .filter(t=>(t._2 != 0.0))
      .groupByKey()
      .map(t=>("Daily_session",masthead,t._1,(t._2.toList.sum/t._2.toList.length.toDouble).toDouble))

      println("session_dur")
//      session_dur.take(5).foreach(println)

//Bounce Rate
      val weekly_bounce_rate = fid_numpage_session
      .map(t=>(t._6,t._3.toDouble))
      .filter(t=>(t._2 == 1.0))
      .map(t=>(t._1,1))
      .reduceByKey(_+_)
      .map(c=>("Week_bounce",masthead,c._1,c._2.toDouble))

      val daily_bounce_rate = fid_numpage_session
      .map(t=>(t._5,t._3.toDouble))
      .filter(t=>(t._2 == 1.0))
      .map(t=>(t._1,1))
      .reduceByKey(_+_)
      .map(c=>("Daily_bounce",masthead,c._1,c._2.toDouble))

      println("bounce_rate")

//Repeat Count
      val weekly_repeat = fid_numpage_session
      .map(t=>((t._6, t._1),1))
      .reduceByKey(_+_)
      .filter(t=>(t._2 > 1))
      .map(t=>(t._1._1, 1))
      .reduceByKey(_+_)
      .map(c=>("Week_repeat",masthead,c._1,c._2.toDouble))

      val daily_repeat = fid_numpage_session
      .map(t=>((t._5, t._1),1))
      .reduceByKey(_+_)
      .filter(t=>(t._2 > 1))
      .map(t=>(t._1._1, 1))
      .reduceByKey(_+_)
      .map(c=>("Daily_repeat",masthead,c._1,c._2.toDouble))

      val visitors_cnt = weekly_cnt_visitors ++ daily_cnt_visitors
      val visits_cnt = weekly_cnt_visits ++ daily_cnt_visits
      val numpage_cnt = weekly_numpage ++ daily_numpage
      val sessiondur_cnt = weekly_session_dur ++ daily_session_dur
      val bounce_cnt = weekly_bounce_rate ++ daily_bounce_rate
      val repeat_cnt = weekly_repeat ++ daily_repeat

      visitors_cnt.repartition(1).saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/view2/" + masthead + "_visitors_wk" +wk)
      visits_cnt.repartition(1).saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/view2/" + masthead + "_visits_wk" +wk)
      numpage_cnt.repartition(1).saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/view2/" + masthead + "_numpage_wk" +wk)
      sessiondur_cnt.repartition(1).saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/view2/" + masthead + "_sessiondur_wk" +wk)
      bounce_cnt.repartition(1).saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/view2/" + masthead + "_bounce_wk" +wk)
      repeat_cnt.repartition(1).saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/view2/" + masthead + "_repeat_wk" +wk)

}

    def toWeek(timestamp:Long): String = {

      val mydate = Calendar.getInstance(TimeZone.getTimeZone("Australia/Sydney"),Locale.GERMANY)
      mydate.setTimeInMillis(timestamp)
      val returndate = mydate.get(Calendar.WEEK_OF_YEAR)
      return returndate.toString
    }

    def toDate_try(timestamp:Long): String = {

      val mydate = Calendar.getInstance(TimeZone.getTimeZone("Australia/Sydney"),Locale.GERMANY)
      mydate.setTimeInMillis(timestamp)
      val returndate = mydate.get(Calendar.DAY_OF_MONTH)
      return returndate.toString
    }

    var b = new ListBuffer[String]()
    var fs:String=""

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
        ys :: split(zs)
      }

    }

    def compressFunctional[A](ls: List[A]): List[A] =
    ls.foldRight(List[A]()) { (h, r) =>
    if (r.isEmpty || r.head != h) h :: r
    else r
  }

  def check(a:List[List[String]]):List[String]= {
    b.clear()
    for(i<-0 until a.length) {b+= (i+1).toString}

    return b.toList
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


                        def deserialise(json:String):Click = {
                          implicit val formats = DefaultFormats

                          parse(json).extract[Click]
                        }

                      }


