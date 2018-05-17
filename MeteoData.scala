package main

import scala.io.Source
import scala.collection.mutable.ListBuffer
import java.io.{IOException, PrintWriter}
import util.control.Breaks._
import org.apache.spark.sql._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset

object MeteoData {

  // class Data describe our data structure 
  case class Data(
                 name: String, year: String, month: Int, tmax: Double,
                 tmin: Double, afdays: Int, rainmm: Double, sunhours: Double
             )

  // write to file helper 
  def writeToFile( content: String, filename: String ) {
    new PrintWriter(filename) {
      write(content); close
    }
  }

  //return list with strings
  //each line contain station name and measure. All values space separated
  def getRawMeteoData(stations : Vector[String]) : ListBuffer[String] = {

    val rawMeteoData: ListBuffer[String] = ListBuffer()
    val headerTag = "degC"

    for (station <- stations) {

      try {

        println("Processing:  " + station)
        val url = String.format("https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/%sdata.txt ", station)
        val arrayOfLines = Source.fromURL(url).mkString.split('\n')
        var headerFound = false

        for (ln <- arrayOfLines) {

          breakable {
            if (!headerFound) {
              if (ln contains (headerTag)) {
                headerFound = true
              }
              break   //means continue here
            }

            val newLn = station + " " + ln
            rawMeteoData+=newLn
          }  //breakable
        }
      } catch {
        case ioe: IOException => println("IOException for station : " + station + "  " + ioe)
        case e: Exception => println("Exception for station "  + station + "  "  + e)
      }
    }
    return rawMeteoData
  }



  def main(args: Array[String]): Unit = {

    val allDataTextFile = "./data/allData.txt"

    // for test scenario hardcodes list of stations is suitable
    val stations = Vector(
      "aberporth", "armagh", "ballypatrick","bradford", "braemar", "camborne", "cambridge", "cardiff", "chivenor", "cwmystwyth",
      "dunstaffnage", "durham", "eastbourne", "eskdalemuir", "heathrow", "hurn", "lerwick", "leuchars", "lowestoft", "manston",
      "nairn", "newtonrigg", "oxford", "paisley", "ringway", "rossonwye", "shawbury", "sheffield", "southampton", "stornoway",
      "suttonbonington", "tiree", "valley", "waddington", "whitby",   "wickairport", "yeovilton"
    );

    //download data and return list of measures with station name
    val rawData = getRawMeteoData(stations)


    writeToFile(rawData.mkString("\n"), allDataTextFile)

    // Initialize SPARK 
    val conf = new SparkConf().setAppName("Meteo Data Analyser")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    //first RDD 
    val rdd = sc.parallelize(rawData)
    println( "RDD [1]: " + rdd.take(1) )

    // print first element
    rdd.take(10).foreach(println)

    //we have RDD   
    val rdd2 = rdd.map(_.trim.replaceAll(" +", " ")).map(_.split(" ").toList)
    rdd2.take(10).foreach(println)
    println("Class name: " + rdd2.getClass)
  
  // Reading data from file here is workabour. 
  // This is the first methd I have found to transform and parse data 

  // val data = sc.textFile(allDataTextFile)  //read as a text file
  //   .map(_.replace("---", "0").replaceAll("-|#|\\*", ""))  //replace special charactes 
  //   .map(_.split("\\s+"))
  //   .map(x =>  // create Data object for each record
  //     Data(x(0), x(1), x(2).toInt, x(3).toDouble, x(4).toDouble, x(5).toInt, x(6).toDouble, x(7).replace("l", "").toDouble)
  //   )

  // read data directly from initial RDD
  val data = rdd 
    .map(_.replace("---", "0").replaceAll("-|#|\\*", ""))  //replace special charactes 
    .map(_.split("\\s+"))
    .map(x =>  // create Data object for each record
      Data(x(0), x(1), x(2).toInt, x(3).toDouble, x(4).toDouble, x(5).toInt, x(6).toDouble, x(7).replace("l", "").toDouble)
    )

  println("data:  " + data )  // print class type here 

  import sqlContext.implicits._

  // Unfortunately I have runtime error on this line
  // Exception in thread "main" java.lang.NoSuchMethodError:
  val dataset = data.toDS()
  dataset.show


  // Here we can answer first questions. 
  // We can group data but name and calculate number of measures for each stations 
  // ....................................................
  // ....................................................

  }
}
