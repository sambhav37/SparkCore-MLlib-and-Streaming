package com.sambhav37.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max
object MaxPrecipitation {
   def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val precipitation = fields(3)
    (stationID, entryType, precipitation)
  }
   def main(args: Array[String]) {
      // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxTemperatures")
    
    val lines = sc.textFile("../1800.csv")
    val parsedLines = lines.map(parseLine)
    val precip = parsedLines.filter(x=>x._2=="PRCP")
    val stationPrecip = precip.map(x=>(x._1,x._3.toFloat))
    val maxPrecipByStation = stationPrecip.reduceByKey((x,y)=> max(x,y))
    
    val results = maxPrecipByStation.collect()
    
    for (result <- results.sorted) {
       val station = result._1
       val maxPrecipitation = result._2
       println(s"$station max precipitation: $maxPrecipitation") 
    }
   }
}