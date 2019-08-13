package com.sambhav37.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
object SumAmount {
  
//  def parseLine(line:String)= {
//    val fields = line.split(",")
//    val customerID = fields(0).toInt
//    val amount = fields(2).toFloat
//    (customerID, amount)
//  }
//  def main(args: Array[String]) {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val sc = new SparkContext("local", "SumAmount")
//    val input = sc.textFile("../customer-orders.csv")
//    val parsed = input.map(parseLine)
//    val reduction = parsed.reduceByKey( (x,y) => x + y ).map(x=>(x._2,x._1)).sortByKey().collect()
//    reduction.foreach(x=>{
//      val customerID=x._2
//      val amount=x._1
//      println(s"$customerID: $amount")
//    })
//  }
  

  
//  def main(args: Array[String]){
//    
//     // Set the log level to only print errors
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    
//     // Create a SparkContext using every core of the local machine
//    val sc = new SparkContext("local[*]", "SumAmount")   
//    
//    val input = sc.textFile("../ml-100k/u.data")
//    val f_list = input.map(x=>x.split("\t")(1).toInt)
//    val reduced = f_list.countByValue()
//    reduced.foreach(println)
//
//  }
}
