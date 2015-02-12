/*
Creator - Mu Sigma

Objective - Read all the data for a masthead -> sessionise it -> enrich IP to identify the country and filter for AU -> calculate the metrics for each month
Inputs - User has to pass one argument - the masthead for which data will be read.

Outputs - The code outputs monthly metrics for the masthead. It is saved in mu-sigma-data folder on S3 with relevant name.

Assumptions in the code - 
1.) Mobile Views are those where filename is of the format - masthead-m-view
	Desktop Views are those where the filename is of the format - masthead-view

Modifications - 
Currently code is not running. It could be due to the number of files read as input. Will modify the code to read only an year at a time to not overwhelm the cluster.

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
//import ua_parser.{Client, Parser}
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex
import Ordering.Implicits._
import Numeric.Implicits._
import scala.math._
import scala.collection.mutable.ListBuffer
import java.net.URL
import scala.util.control._
import scala.io.Source
//import com.github.nscala_time.time.Imports._
import org.json4s.DefaultFormats

import java.util.{Calendar, Locale, TimeZone}


object forecast {

    type Click = Map[String, String]

  def main(args:Array[String]) {
//set the buffer and kryo serial properties. This was set since the code was giving a buffer error
    val conf = new SparkConf().setAppName("Forecast").set("spark.io.compression.codec","org.apache.spark.io.LZ4CompressionCodec").set("spark.kryoserializer.buffer.mb","128")

    val sc = new SparkContext(conf)
    var masthead = args(0).toString

//function that reads and sesionizes the data
    def sessionise(clicksLocation: String): RDD[Click] = {
      var read_text = sc.textFile(clicksLocation,50)
      val clicks: RDD[Click] = (read_text
        .filter((l: String) => !(l contains "00000000-0000-0000-0000-000000000000"))
        .map(expandRecord(_))
        )
      .repartition(150)

//Group all the timestamps for a tracking ID, to be used for sessionising the data
var groupByTrackingIdsAndDate = clicks
.map(c => {
    var timestamp = c.getOrElse("timestamp", "")
    var startOfDay = toDate_try(timestamp.toLong)
    ((c.getOrElse("trackingId", ""), startOfDay), timestamp)
  })
.groupByKey()
.map(t => (t._1, t._2.toList.sorted))
.map(t => (t._1, split(t._2)))

//Flatten the list to get list of timestamps as separate rows
var track_individual_list = groupByTrackingIdsAndDate.flatMap(t => (for (xs <- t._2) yield (t._1, xs)))

//Map the session ID back to the original click data. Assumption - negligible chances of a tracking ID having two clicks at the exact same timestamp
var withsessionid = track_individual_list
.map { case ((trackingId, date), timestamps) => ((trackingId, timestamps),
  (
    timestamps.head + trackingId + date,
    timestamps.head,
    timestamps.last,
    (timestamps.last.toLong - timestamps.head.toLong) / 1000,
    timestamps.length
  ))
}
.flatMap { case ((trackingId, timestamps), t) => {
  for (timestamp <- timestamps)
    yield ((trackingId, timestamp), t)
}
}

//Add the sessionID and referer to the click data using updated statement
var sessionised_clicks = clicks
.map(c => ((c.getOrElse("trackingId", ""), c.getOrElse("timestamp", "")), c))
.join(withsessionid)
.map { case ((trackingId, timestamp), (click, sessionInfo)) => (click.updated("sessionId", sessionInfo._1.toString)
  .updated("sessionDuration", sessionInfo._4.toString)
  .updated("sessionStart", sessionInfo._2.toString)
  .updated("sessionEnd", sessionInfo._3.toString)
  .updated("numPage", sessionInfo._5.toString))
}
sessionised_clicks

  }

  /*
   * Single function to add location fields to an RDD of clicks
   * input is the RDD of clicks to enrich
   * and the (hadoopable) location to find the ip location file
   */
  def addLocFields(clicks:RDD[Click],ipToCityLoc:String): RDD[Click] = {

    // first, get and broadcast the ip ranges

    val (ipToCity:RDD[((Long,Long),(Long, Array[String]))]) = (sc.hadoopFile(ipToCityLoc, classOf[TextInputFormat], classOf[LongWritable],classOf[Text])
      .map( pair => (pair._1.get(), pair._2.toString) )
      .filter(
        (line:(Long,String)) => !(line._2 contains ":")
      )
      .mapValues(
        (line:String) => line.split(",")
          .map(
            (word:String) => word.filterNot(_ == '"')
          )
      )
      .map(
        (line:(Long,Array[String])) => ((ipToLong(line._2(0)),ipToLong(line._2(1))), line)
      )
      )

    val ipRanges:Broadcast[Array[(Long,Long)]] = sc.broadcast(ipToCity
      .map(_._1)
      .collect()
      .sortBy(_._1)
    )

    //Now, enrich the clicks
    (clicks
      .map(
        (click:Click) => (getAddressRange(ipRanges.value, ipToLong(click.getOrElse("IpAddress", "203.192.146.201"))),click)
      )
      .join(ipToCity)
      .map(
        (data:((Long,Long),(Click,(Long,Array[String])))) => (data._2._1
          .updated("Country", data._2._2._2(2))
          .updated("Region", data._2._2._2(3))
          .updated("City", data._2._2._2(4))
          .updated("ipBlock", data._2._2._2(0) + " - " + data._2._2._2(1))
          .updated("ipBlockIndex", data._2._2._1.toString)
          )
      )
      )
  }

//function to calculate the page views and visits for each month and save it
def calculate_metrics(clicks:RDD[Click], type_ext:String) = {
  var fid_numpage_session = clicks
    .map(c=>(c,c.getOrElse("timestamp",""),c.getOrElse("sessionId",""), c.getOrElse("Country","")))
    .filter(t=>(t._2.equals(t._3) && t._4 == "AU"))
    .map(t=>(t._1))
    .map(c=>(c.getOrElse("sessionId","").toString,c.getOrElse("numPage","").toString, c.getOrElse("site","").toString, toMonth(c.getOrElse("timestamp","").toString.toLong)))

  var visits =  fid_numpage_session
    .map(t=>((t._3, t._4),1))
    .reduceByKey(_+_)
    .map(c=>("Month_visit", masthead, "Overall", c._1._1, c._1._2, c._2.toDouble))

  var numpage = fid_numpage_session
    .map(t=>((t._3, t._4),t._2.toDouble))
    .reduceByKey(_+_)
    .map(t=>("Month_pageviews", masthead, "Overall", t._1._1, t._1._2, t._2.toDouble))

  (numpage ++ visits).repartition(1).saveAsTextFile("s3n://ffx-analytics/mu-sigma-data/forecast/" + masthead + "_metrics_" + type_ext, classOf[GzipCodec])
  
}

  var IpDbFile = "s3n://ffx-analytics/dbip-city-2014-09.csv.gz"

  calculate_metrics(addLocFields(sessionise("s3n://ffx-analytics/all-interactions/*"+ masthead + "*view*"), IpDbFile),"overall")
  calculate_metrics(addLocFields(sessionise("s3n://ffx-analytics/all-interactions/*"+ masthead + "-m-view*"), IpDbFile),"mobile")
  calculate_metrics(addLocFields(sessionise("s3n://ffx-analytics/all-interactions/*"+ masthead + "-view*"), IpDbFile),"desktop")



}

def toDate_try(timestamp: Long): String = {

  val mydate = Calendar.getInstance(TimeZone.getTimeZone("Australia/Sydney"), Locale.GERMANY)
  mydate.setTimeInMillis(timestamp)
  val returndate = mydate.get(Calendar.DAY_OF_YEAR) + " ' " + mydate.get(Calendar.YEAR)
  return returndate.toString
}
var IpRegex = new Regex("""([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})""")
  def ipToLong(ip:String):Long = ip match {
    case IpRegex(a,b,c,d) => Array(a,b,c,d).map(_.toLong).foldLeft(0L)((a: Long, b: Long) => (a << 8) | (b & 0xff))
    case _                => 1L
  }

  // Binary search of the address range
  def getAddressRange(ranges:Array[(Long, Long)], addr: Long):(Long,Long) = {
    def getAddressRangeHelper(ranges:Array[(Long,Long)], start:Int, end:Int, addr:Long):(Long,Long) = {
      val midpoint = start + (end-start)/2
      if (addr < ranges(midpoint)._1)
        getAddressRangeHelper(ranges, start, midpoint-1, addr)
      else if (addr > ranges(midpoint)._2)
        getAddressRangeHelper(ranges, midpoint+1, end, addr)
      else
        ranges(midpoint)
    }
    getAddressRangeHelper(ranges, 0, ranges.size-1, addr)
  }

  // Halve a list of tuples
  def halveRange(in:Array[(Long,Long)]):Array[(Long,Long)] = {
    def halveRangeRec(out:List[(Long,Long)], in:List[(Long,Long)]):List[(Long,Long)] = in match {
      case List()      => out
      case x::List()   => x::out
      case x::y::xs    => {
        println(x)
        halveRangeRec((x._1,y._2)::out, xs)
      }
    }

    halveRangeRec(List(), in.toList).reverse.toArray
  }


    def toMonth(timestamp:Long): String = {

      val mydate = Calendar.getInstance(TimeZone.getTimeZone("Australia/Sydney"), Locale.GERMANY)
      mydate.setTimeInMillis(timestamp)
      val returndate = mydate.get(Calendar.MONTH).toString + "-" + mydate.get(Calendar.YEAR).toString
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

    def parseWithErrors(jsonRecord: String): Option[Map[String, JValue]] = {

      implicit val formats = DefaultFormats

      try {
        Some(parse(jsonRecord).extract[Map[String, JValue]])
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

    def expandRecord(jsonRecord: String): Map[String, String] = {

      val digest = MessageDigest.getInstance("MD5")

      val partParsed: Option[Map[String, JValue]] = parseWithErrors(jsonRecord)
      // Now, walk partParsed and convert each of the JValues to their primitive types.
      // This is to circumvent https://github.com/json4s/json4s/issues/124

      def reparse(v: JValue): Any = v match {
        case JString(s) => s
        case JInt(i) => i.toString()
        case JArray(o) => (o
          .asInstanceOf[List[JObject]]
          .map(
            (pair: JObject) => {
              // we know that the pair has name, value keys
              val p = (pair
                .obj
                .toMap
                .mapValues(_.values.asInstanceOf[String])
              )

            Map[String, String](p("name") -> p("value"))
          }
        )
      .foldLeft(Map[String, String]())(_ ++ _)
    )
  case _ => v
}

partParsed match {
  case Some(s) => {
    val withExtras = s.mapValues(reparse)
    val trackingId = digest.digest(withExtras("trackingId").asInstanceOf[String].getBytes).map("%02x".format(_)).mkString
      val extras = withExtras.getOrElse("extras", Map()).asInstanceOf[Map[String, String]]
      (withExtras - "extras").updated("trackingId", trackingId).asInstanceOf[Map[String, String]] ++ extras
    }
    case None => Map[String, String]()
  }
}

def serialise(click: Map[String, String]): String = {

  implicit val formats = DefaultFormats
  write(click)
}

}
